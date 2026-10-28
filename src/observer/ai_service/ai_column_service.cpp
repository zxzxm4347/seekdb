/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#define USING_LOG_PREFIX SERVER
#include "observer/ai_service/ai_column_service.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_ai_column_schema.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "observer/omt/ob_ai_service.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "common/timezone/ob_time_convert.h"
#include "lib/worker.h"
#include <algorithm>
#include <atomic>
#include <charconv>
#include <cmath>
#include <limits>
#include <sstream>
#include <set>
#include <vector>
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace oceanbase { namespace observer {
using namespace common;
using namespace share::schema;
namespace ac = share::schema::ai_column;
namespace {
constexpr const char *TASKS = "oceanbase.__all_ai_column_task";
constexpr int64_t TASK_PENDING = 0, TASK_RUNNING = 1, TASK_RETRY = 3, TASK_READY = 4, TASK_FAILED = 5;
struct Column { uint64_t id; std::string name, state, request, spec; ac::Definition definition; };
struct Task {
  uint64_t table_id = 0, column_id = 0;
  std::string request, rowkey, version, input, result;
  int64_t state = 0, retries = 0, eligible = 0;
  bool input_null = false, result_null = false;
};
std::string str(const ObString &value) { return {value.ptr() ? value.ptr() : "", static_cast<size_t>(value.length())}; }
void require(bool condition, const char *message) { if (!condition) throw ac::Error(message); }
int write_sql(ObISQLClient &client, const std::string &sql, int64_t *changed = nullptr)
{
  int64_t count = 0;
  int ret = client.write(sql.c_str(), count);
  if (changed) *changed = count;
  return ret;
}
std::string group_where(uint64_t table_id, const std::string &request)
{ return "table_id=" + std::to_string(table_id) + " AND request_id=" + ac::literal(request); }
std::string rowkey_expression(const ObTableSchema &table, const std::string &record)
{
  // Heap storage assigns its native hidden primary key after BEFORE INSERT.
  // A separate trigger-assigned identity is available immediately and survives
  // vector-index maintenance that can replace the physical heap key.
  if (table.is_table_with_hidden_pk_column()
      && table.get_column_schema(ObString::make_string(ac::ROW_ID_COLUMN))) {
    const std::string name = ac::identifier(ac::ROW_ID_COLUMN);
    return "CONCAT(" + ac::literal(name + " = CONVERT(0x") + ",HEX(" + record + "." + name
        + "),' USING utf8mb4)')";
  }
  std::string sql = "CONCAT(";
  const auto &keys = table.get_rowkey_info();
  require(keys.get_size() > 0, "AI COLUMN: table has no stable row identity");
  for (int64_t i = 0; i < keys.get_size(); ++i) {
    const auto *column = table.get_column_schema(keys.get_column(i)->column_id_);
    require(column, "AI COLUMN: missing rowkey column");
    const std::string name = str(column->get_column_name_str());
    const std::string value = record + "." + ac::identifier(name);
    if (i) sql += "," + ac::literal(" AND ") + ",";
    sql += ac::literal(ac::identifier(name) + " = ") + ",";
    if (ob_is_integer_type(column->get_data_type()) || ob_is_numeric_type(column->get_data_type())) sql += "CAST(" + value + " AS CHAR)";
    else sql += "CONCAT('CONVERT(0x',HEX(" + value + "),' USING utf8mb4)')";
  }
  return sql + ")";
}
std::vector<Column> columns_of(const ObTableSchema &table)
{
  std::vector<Column> columns;
  for (int64_t i = 0; i < table.get_column_count(); ++i) {
    const auto *column = table.get_column_schema_by_idx(i);
    if (!column || !column->is_ai_column()) continue;
    Column value; value.id = column->get_column_id(); value.name = str(column->get_column_name_str());
    value.definition = ac::Definition::deserialize(str(column->get_ai_column_info()));
    const auto *state = table.get_column_schema(value.definition.state_column_id);
    const auto *request = table.get_column_schema(value.definition.request_column_id);
    const auto *spec = table.get_column_schema(value.definition.result_spec_column_id);
    require(state && request && spec, "AI COLUMN: missing hidden state columns");
    value.state = str(state->get_column_name_str()); value.request = str(request->get_column_name_str()); value.spec = str(spec->get_column_name_str());
    columns.push_back(std::move(value));
  }
  return columns;
}
std::string changed_condition(const ObTableSchema &table, const Column &column)
{
  std::string condition = "NOT(NEW." + ac::identifier(column.request) + " <=> OLD." + ac::identifier(column.request) + ")";
  std::set<uint64_t> dependencies;
  for (const auto &dependency : column.definition.dependencies) dependencies.insert(dependency.column_id);
  for (int64_t i = 0; i < table.get_rowkey_info().get_size(); ++i) {
    const auto *key = table.get_column_schema(table.get_rowkey_info().get_column(i)->column_id_);
    if (key && !key->is_hidden()) dependencies.insert(key->get_column_id());
  }
  for (uint64_t id : dependencies) {
    const auto *dependency = table.get_column_schema(id); require(dependency, "AI COLUMN: missing dependency");
    const auto name = ac::identifier(str(dependency->get_column_name_str()));
    condition += " OR NOT(NEW." + name + " <=> OLD." + name + ")";
  }
  return "(" + condition + ")";
}
std::string invalidate(const Column &column)
{
  return "SET NEW." + ac::identifier(column.name) + "=NULL; SET NEW." + ac::identifier(column.state) + "=0; SET NEW."
      + ac::identifier(column.request) + "=ai_group_request; SET NEW." + ac::identifier(column.spec)
      + "=" + ac::literal(column.definition.definition_version) + "; ";
}
std::string enqueue(const ObTableSchema &table, const Column &column)
{
  return "INSERT INTO " + std::string(TASKS)
      + " (table_id,request_id,column_id,rowkey_predicate,definition_version,model_name,input_text,state,retry_count,eligible_ts) VALUES ("
      + std::to_string(table.get_table_id()) + ",NEW." + ac::identifier(column.request) + "," + std::to_string(column.id) + ","
      + rowkey_expression(table, "NEW") + "," + ac::literal(column.definition.definition_version) + "," + ac::literal(column.definition.model)
      + "," + ac::qualify_input(column.definition.input_expression, column.definition.dependencies, "NEW") + ",0,0,NOW(6)) ON DUPLICATE KEY UPDATE rowkey_predicate=VALUES(rowkey_predicate),"
        "definition_version=VALUES(definition_version),model_name=VALUES(model_name),input_text=VALUES(input_text),"
        "state=0,retry_count=0,eligible_ts=VALUES(eligible_ts),claim_id=NULL,lease_until=NULL,result_text=NULL,error_code=0; ";
}
std::string trim(std::string text)
{
  size_t begin = text.find_first_not_of(" \t\r\n"), end = text.find_last_not_of(" \t\r\n");
  return begin == std::string::npos ? std::string() : text.substr(begin, end - begin + 1);
}
bool valid_date_prefix(const std::string &value)
{
  if (value.size() < 10 || value[4] != '-' || value[7] != '-') return false;
  for (size_t i : {0,1,2,3,5,6,8,9}) if (value[i] < '0' || value[i] > '9') return false;
  int year = (value[0]-'0')*1000 + (value[1]-'0')*100 + (value[2]-'0')*10 + value[3]-'0';
  int month = (value[5]-'0')*10 + value[6]-'0', day = (value[8]-'0')*10 + value[9]-'0';
  const int days[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
  if (year < 1 || month < 1 || month > 12) return false;
  return day >= 1 && day <= days[month] + (month == 2 && year%4 == 0 && (year%100 != 0 || year%400 == 0));
}
int validate_output(const ObColumnSchemaV2 &column, const ac::Definition &definition, std::string &text)
{
  if (text.size() > 1024 * 1024) return OB_SIZE_OVERFLOW;
  const auto type = column.get_data_type();
  std::string numeric = trim(text);
  if (definition.dimension || column.is_json()) {
    rapidjson::Document json; json.Parse(text.data(), text.size());
    if (json.HasParseError()) return OB_INVALID_ARGUMENT;
    if (definition.dimension) {
      if (!json.IsArray() || json.Size() != static_cast<size_t>(definition.dimension)) return OB_ERR_INVALID_VECTOR_DIM;
      for (const auto &value : json.GetArray()) if (!value.IsNumber() || !std::isfinite(value.GetDouble())
          || std::abs(value.GetDouble()) > std::numeric_limits<float>::max()) return OB_INVALID_ARGUMENT;
      rapidjson::StringBuffer buffer; rapidjson::Writer<rapidjson::StringBuffer> writer(buffer); json.Accept(writer);
      text.assign(buffer.GetString(), buffer.GetSize());
    }
  } else if (ob_is_integer_type(type)) {
    const char *begin = numeric.data(), *end = begin + numeric.size();
    if (begin == end) return OB_INVALID_ARGUMENT;
    if (*begin == '+') ++begin;
    const bool is_unsigned = ob_is_unsigned_type(type);
    if (is_unsigned) {
      uint64_t value; auto parsed = std::from_chars(begin, end, value);
      if (parsed.ec != std::errc() || parsed.ptr != end || value > UINT_MAX_VAL[type]) return OB_INVALID_ARGUMENT;
    } else {
      int64_t value; auto parsed = std::from_chars(begin, end, value);
      if (parsed.ec != std::errc() || parsed.ptr != end || value < INT_MIN_VAL[type] || value > INT_MAX_VAL[type]) return OB_INVALID_ARGUMENT;
    }
  } else if (ob_is_numeric_type(type)) {
    double value; auto parsed = std::from_chars(numeric.data(), numeric.data() + numeric.size(), value);
    if (parsed.ec != std::errc() || parsed.ptr != numeric.data() + numeric.size() || !std::isfinite(value)) return OB_INVALID_ARGUMENT;
    if ((type == ObFloatType || type == ObUFloatType) && std::abs(value) > std::numeric_limits<float>::max()) return OB_INVALID_ARGUMENT;
    if (ob_is_unsigned_type(type) && value < 0) return OB_INVALID_ARGUMENT;
    if (ob_is_number_tc(type) && column.get_data_precision() > 0
        && std::abs(value) >= std::pow(10.0, column.get_data_precision() - column.get_data_scale())) return OB_INVALID_ARGUMENT;
  } else if (type == ObDateType || type == ObMySQLDateType) {
    if (text.size() != 10 || !valid_date_prefix(text)) return OB_INVALID_ARGUMENT;
    int32_t value; int ret = ObTimeConverter::str_to_date(ObString(text.size(), text.data()), value); if (ret) return ret;
  } else if (type == ObDateTimeType || type == ObMySQLDateTimeType) {
    if (text.size() < 19 || text.size() > 26 || !valid_date_prefix(text) || text[10] != ' '
        || text[13] != ':' || text[16] != ':') return OB_INVALID_ARGUMENT;
    for (size_t i : {11,12,14,15,17,18}) if (text[i] < '0' || text[i] > '9') return OB_INVALID_ARGUMENT;
    if ((text[11]-'0')*10+text[12]-'0' > 23 || (text[14]-'0')*10+text[15]-'0' > 59
        || (text[17]-'0')*10+text[18]-'0' > 59) return OB_INVALID_ARGUMENT;
    if (text.size() > 19) {
      if (text.size() == 20 || text[19] != '.') return OB_INVALID_ARGUMENT;
      for (size_t i = 20; i < text.size(); ++i) if (text[i] < '0' || text[i] > '9') return OB_INVALID_ARGUMENT;
    }
    int64_t value; ObTimeConvertCtx time(nullptr, false);
    int ret = ObTimeConverter::str_to_datetime(ObString(text.size(), text.data()), time, value); if (ret) return ret;
  } else if (column.is_string_type()) {
    int64_t bytes = 0; int ret = column.get_byte_length(bytes, false);
    if (ret) return ret;
    if (text.size() > static_cast<size_t>(bytes)) return OB_ERR_DATA_TOO_LONG;
    if (!ob_is_text_tc(type) && ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN, text.data(), text.size())
        > static_cast<size_t>(column.get_data_length())) return OB_ERR_DATA_TOO_LONG;
  } else return OB_NOT_SUPPORTED;
  return OB_SUCCESS;
}
int model_call(const ac::Definition &definition, const std::string &input, std::string &output)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AiColumnCall");
  ObAIFuncExprInfo *info = nullptr;
  omt::ObAiServiceGuard guard;
  const share::ObAiModelEndpointInfo *endpoint = nullptr;
  ObString model(definition.model.size(), definition.model.data());
  ObString content(input.size(), input.data());
  ObString result;
  if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, model, info))) {
  } else if (!info) ret = OB_ERR_UNEXPECTED;
  else if (OB_FAIL(guard.get_ai_endpoint_by_ai_model_name(model, endpoint))) {
  } else if (!endpoint) ret = OB_ERR_UNEXPECTED;
  else {
    ObAIFuncModel function(allocator, *info, *endpoint);
    if (definition.dimension) {
      ObJsonObject *config = nullptr;
      ObJsonInt *dimension = nullptr;
      if (definition.request_dimension) {
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
        } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, definition.request_dimension, dimension))) {
        } else ret = config->add("dimensions", dimension);
      }
      if (OB_SUCC(ret)) ret = function.call_dense_embedding(content, config, result);
    }
    else ret = function.call_completion_with_system(content,
        ObString(definition.type_hint.size(), definition.type_hint.data()), nullptr, result);
    if (OB_SUCC(ret)) output = str(result);
  }
  return ret;
}
int read_tasks(ObISQLClient &client, uint64_t table_id, const std::string &request, std::vector<Task> &tasks)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy::MySQLResult result;
  std::string sql = "SELECT column_id,rowkey_predicate,definition_version,CAST(input_text AS CHAR) AS input_text,state,retry_count,"
      "CAST(result_text AS CHAR) AS result_text,eligible_ts<=NOW(6) AS eligible FROM " + std::string(TASKS) + " WHERE " + group_where(table_id, request) + " ORDER BY column_id";
  if (OB_FAIL(client.read(result, sql.c_str()))) return ret;
  auto *rows = result.get_result(); if (!rows) return OB_ERR_UNEXPECTED;
  while (OB_SUCC(ret) && OB_SUCC(ret = rows->next())) {
    if (tasks.size() >= 64) return OB_SIZE_OVERFLOW;
    Task task; task.table_id = table_id; task.request = request;
    int64_t column_id = 0; ObString rowkey, version, input, output;
    if (OB_FAIL(rows->get_int("column_id", column_id))) break;
    if (OB_FAIL(rows->get_varchar("rowkey_predicate", rowkey))) break;
    if (OB_FAIL(rows->get_varchar("definition_version", version))) break;
    int input_ret = rows->get_varchar("input_text", input);
    if (input_ret != OB_SUCCESS && input_ret != OB_ERR_NULL_VALUE) { ret = input_ret; break; }
    int output_ret = rows->get_varchar("result_text", output);
    if (output_ret != OB_SUCCESS && output_ret != OB_ERR_NULL_VALUE) { ret = output_ret; break; }
    if (OB_FAIL(rows->get_int("state", task.state))) break;
    if (OB_FAIL(rows->get_int("retry_count", task.retries))) break;
    if (OB_FAIL(rows->get_int("eligible", task.eligible))) break;
    task.column_id = column_id; task.rowkey = str(rowkey); task.version = str(version); task.input = str(input);
    task.input_null = input_ret == OB_ERR_NULL_VALUE; task.result_null = output_ret == OB_ERR_NULL_VALUE; task.result = str(output);
    tasks.push_back(std::move(task));
  }
  return ret == OB_ITER_END ? OB_SUCCESS : ret;
}
}

AiColumnService &AiColumnService::instance() { static AiColumnService service; return service; }
int AiColumnService::start_service()
{
  std::lock_guard<std::mutex> lock(lifecycle_mutex_);
  if (started_) return OB_SUCCESS;
  recovered_.store(false); has_ai_columns_.store(false); set_thread_count(4);
  set_run_wrapper(lib::Threads::get_expect_run_wrapper());
  int ret = start(); if (OB_SUCC(ret)) started_ = true; return ret;
}
void AiColumnService::stop_service()
{
  std::lock_guard<std::mutex> lock(lifecycle_mutex_);
  if (started_) { stop(); wait(); destroy(); started_ = false; }
}
int AiColumnService::install_table(uint64_t table_id, bool initialize_rows)
{
  if (!GCTX.schema_service_ || !GCTX.sql_proxy_) return OB_NOT_INIT;
  {
    ObSchemaGetterGuard fast_guard;
    const ObTableSchema *fast_table = nullptr;
    int ret = OB_SUCCESS;
    if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(fast_guard))) return ret;
    if (OB_FAIL(fast_guard.get_table_schema(table_id, fast_table))) return ret;
    if (!fast_table) return OB_TABLE_NOT_EXIST;
    bool has_ai = false;
    for (int64_t i = 0; i < fast_table->get_column_count(); ++i) {
      const auto *col = fast_table->get_column_schema_by_idx(i);
      if (col && col->is_ai_column()) { has_ai = true; break; }
    }
    if (!has_ai) return OB_SUCCESS;
    has_ai_columns_.store(true);
  }
  std::lock_guard<std::mutex> lock(install_mutex_);
  int ret = OB_SUCCESS;
  try {
    ObSchemaGetterGuard guard;
    const ObTableSchema *table = nullptr;
    const ObDatabaseSchema *database = nullptr;
    if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) return ret;
    if (OB_FAIL(guard.get_table_schema(table_id, table))) return ret;
    if (!table) return OB_TABLE_NOT_EXIST;
    auto columns = columns_of(*table); if (columns.empty()) return OB_SUCCESS;
    if (OB_FAIL(guard.get_database_schema(table->get_database_id(), database))) return ret;
    if (!database) return OB_ERR_UNEXPECTED;
    const std::string db = ac::identifier(str(database->get_database_name_str()));
    const std::string target = db + "." + ac::identifier(str(table->get_table_name_str()));
    const std::string base = "__ai_column_" + std::to_string(table_id) + "_";
    uint64_t generation = 1469598103934665603ULL;
    for (const auto &column : columns) for (unsigned char c : column.definition.definition_version) {
      generation ^= c; generation *= 1099511628211ULL;
    }
    const std::string prefix = base + "v2_" + std::to_string(generation) + "_";
    std::set<std::string> existing;
    for (int64_t i = 0; i < table->get_trigger_list().count(); ++i) {
      const ObTriggerInfo *trigger = nullptr;
      if (OB_FAIL(guard.get_trigger_info(table->get_trigger_list().at(i), trigger))) return ret;
      if (trigger) existing.insert(str(trigger->get_trigger_name()));
    }
    const bool heap_identity = table->is_table_with_hidden_pk_column()
        && table->get_column_schema(ObString::make_string(ac::ROW_ID_COLUMN));
    if (heap_identity) {
      const std::string index_name = "__ai_row_lookup_" + std::to_string(table_id);
      bool indexed = false;
      for (int64_t i = 0; i < table->get_simple_index_infos().count(); ++i) {
        const auto &index = table->get_simple_index_infos().at(i);
        const ObTableSchema *schema = nullptr;
        if (OB_FAIL(guard.get_table_schema(index.table_id_, schema))) return ret;
        ObString name;
        if (schema && OB_SUCC(schema->get_index_name(name)) && str(name) == index_name) indexed = true;
      }
      if (!indexed && OB_FAIL(write_sql(*GCTX.sql_proxy_, "CREATE UNIQUE INDEX " + ac::identifier(index_name)
          + " ON " + target + "(" + ac::identifier(ac::ROW_ID_COLUMN) + ")"))) return ret;
    }
    const std::string events[] = {"BEFORE INSERT", "BEFORE UPDATE", "AFTER INSERT", "AFTER UPDATE", "AFTER DELETE"};
    const std::string suffix[] = {"bi", "bu", "ai", "au", "ad"};
    for (size_t event = 0; event < 5 && OB_SUCC(ret); ++event) {
      const std::string name = db + "." + ac::identifier(prefix + suffix[event]);
      if (existing.count(prefix + suffix[event])) continue;
      std::string sql = "CREATE TRIGGER " + name + " " + events[event] + " ON " + target + " FOR EACH ROW BEGIN ";
      if (event <= 1) sql += "DECLARE ai_group_request VARCHAR(64) DEFAULT UUID(); ";
      if (event <= 1 && heap_identity) {
        const std::string identity = "NEW." + ac::identifier(ac::ROW_ID_COLUMN);
        sql += "IF " + identity + " IS NULL THEN SET " + identity + "=UUID(); END IF; ";
      }
      if (event == 4) {
        sql += "DELETE FROM " + std::string(TASKS) + " WHERE table_id=" + std::to_string(table_id)
            + " AND rowkey_predicate=" + rowkey_expression(*table, "OLD") + "; ";
      } else for (const auto &column : columns) {
        if (event == 0) sql += invalidate(column);
        else if (event == 1) sql += "IF " + changed_condition(*table, column) + " THEN " + invalidate(column) + "END IF; ";
        else if (event == 2) sql += enqueue(*table, column);
        else {
          sql += "IF NOT(NEW." + ac::identifier(column.request) + " <=> OLD." + ac::identifier(column.request) + ") THEN ";
          sql += "DELETE FROM " + std::string(TASKS) + " WHERE table_id=" + std::to_string(table_id)
              + " AND column_id=" + std::to_string(column.id) + " AND rowkey_predicate=" + rowkey_expression(*table, "OLD") + "; ";
          sql += enqueue(*table, column) + "END IF; ";
        }
      }
      if (event == 3 && table->is_table_with_hidden_pk_column()) {
        const auto old_key = rowkey_expression(*table, "OLD"), new_key = rowkey_expression(*table, "NEW");
        sql += "IF NOT(" + old_key + " <=> " + new_key + ") THEN UPDATE " + TASKS
            + " SET rowkey_predicate=" + new_key + " WHERE table_id=" + std::to_string(table_id)
            + " AND rowkey_predicate=" + old_key + "; END IF; ";
      }
      sql += "END";
      if (OB_FAIL(write_sql(*GCTX.sql_proxy_, sql))) LOG_WARN("cannot install AI column trigger", K(ret), "sql", sql.c_str());
    }
    // Install the complete new generation before removing any previous generation.
    // Overlapping AFTER triggers enqueue the same request idempotently.
    for (const auto &name : existing) {
      if (OB_SUCC(ret) && name.compare(0, base.size(), base) == 0 && name.compare(0, prefix.size(), prefix) != 0) {
        ret = write_sql(*GCTX.sql_proxy_, "DROP TRIGGER " + db + "." + ac::identifier(name));
      }
    }
    if (OB_SUCC(ret) && initialize_rows) {
      std::string assignments, predicate;
      for (const auto &column : columns) {
        const std::string request = ac::identifier(column.request);
        const std::string missing = "(" + ac::identifier(column.spec) + " IS NULL OR " + ac::identifier(column.spec) + "<>"
            + ac::literal(column.definition.definition_version) + " OR " + request + " IS NULL)";
        if (!assignments.empty()) { assignments += ","; predicate += " OR "; }
        assignments += request + "=IF(" + missing + ",UUID()," + request + ")";
        predicate += missing;
      }
      ret = write_sql(*GCTX.sql_proxy_, "UPDATE " + target + " t SET " + assignments + " WHERE " + predicate);
    }
    if (OB_SUCC(ret) && initialize_rows) {
      // Discover missing queue members with a read, then repair by row/request.
      // Referring to the queue in the triggering UPDATE itself would violate
      // MySQL's mutating-table rule when the AFTER trigger enqueues its task.
      for (;;) {
        struct Repair { std::string rowkey; std::vector<std::pair<size_t, std::string>> members; };
        std::vector<Repair> repairs;
        std::string select = "SELECT " + rowkey_expression(*table, "t") + " AS ai_rowkey", missing;
        for (size_t i = 0; i < columns.size(); ++i) {
          const auto &column = columns[i];
          const std::string request = "t." + ac::identifier(column.request);
          const std::string absent = "(t." + ac::identifier(column.state) + "=0 AND " + request
              + " IS NOT NULL AND NOT EXISTS (SELECT 1 FROM " + TASKS + " q WHERE q.table_id="
              + std::to_string(table_id) + " AND q.column_id=" + std::to_string(column.id)
              + " AND q.request_id=" + request + "))";
          select += "," + request + " AS req_" + std::to_string(i) + "," + absent + " AS missing_" + std::to_string(i);
          if (!missing.empty()) missing += " OR "; missing += absent;
        }
        select += " FROM " + target + " t WHERE " + missing + " LIMIT 256";
        {
          ObMySQLProxy::MySQLResult result;
          if (OB_FAIL(GCTX.sql_proxy_->read(result, select.c_str()))) break;
          auto *rows = result.get_result(); if (!rows) { ret = OB_ERR_UNEXPECTED; break; }
          while (OB_SUCC(ret = rows->next())) {
            Repair repair; ObString key;
            if (OB_FAIL(rows->get_varchar("ai_rowkey", key))) break;
            repair.rowkey = str(key);
            for (size_t i = 0; OB_SUCC(ret) && i < columns.size(); ++i) {
              int64_t absent = 0; ObString request;
              if (OB_FAIL(rows->get_int(("missing_" + std::to_string(i)).c_str(), absent))) break;
              if (absent) {
                if (OB_FAIL(rows->get_varchar(("req_" + std::to_string(i)).c_str(), request))) break;
                repair.members.emplace_back(i, str(request));
              }
            }
            if (OB_SUCC(ret)) repairs.push_back(std::move(repair)); else break;
          }
          if (ret == OB_ITER_END) ret = OB_SUCCESS;
        }
        if (OB_FAIL(ret) || repairs.empty()) break;
        for (const auto &repair : repairs) {
          std::string assignments, guards;
          for (const auto &member : repair.members) {
            const auto &column = columns[member.first];
            const std::string request = ac::identifier(column.request);
            const std::string guard = "(" + request + "=" + ac::literal(member.second) + " AND " + ac::identifier(column.state) + "=0)";
            if (!assignments.empty()) { assignments += ","; guards += " OR "; }
            assignments += request + "=IF(" + guard + ",UUID()," + request + ")"; guards += guard;
          }
          if (OB_FAIL(write_sql(*GCTX.sql_proxy_, "UPDATE " + target + " SET " + assignments + " WHERE ("
              + repair.rowkey + ") AND (" + guards + ")"))) break;
        }
        if (OB_FAIL(ret)) break;
      }
    }
  } catch (const std::bad_alloc &) { ret = OB_ALLOCATE_MEMORY_FAILED; }
  catch (const std::exception &error) { ret = OB_ERR_UNEXPECTED; LOG_WARN("invalid AI column definition", K(ret), "error", error.what()); }
  return ret;
}

int AiColumnService::recover()
{
  if (!GCTX.sql_proxy_ || !GCTX.schema_service_) return OB_NOT_INIT;
  if (!GCTX.sys_package_ready_) return OB_EAGAIN;
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> ai_table_ids;
  {
    ObSchemaGetterGuard guard;
    ObSEArray<const ObTableSchema *, 16> tables;
    if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) return ret;
    if (OB_FAIL(guard.get_table_schemas_in_runtime(tables))) return ret;
    for (const auto *table : tables) {
      if (!table || table->is_index_table() || table->is_view_table() || table->is_in_recyclebin()) continue;
      bool ai = false;
      for (int64_t i = 0; i < table->get_column_count(); ++i) {
        const auto *col = table->get_column_schema_by_idx(i);
        if (col && col->is_ai_column()) { ai = true; break; }
      }
      if (ai && OB_FAIL(ai_table_ids.push_back(table->get_table_id()))) return ret;
    }
  }
  if (ai_table_ids.empty()) return OB_SUCCESS;
  has_ai_columns_.store(true);
  ret = write_sql(*GCTX.sql_proxy_, "UPDATE " + std::string(TASKS)
      + " SET claim_id=NULL,lease_until=NULL,state=IF(state=1,0,state) WHERE claim_id IS NOT NULL OR state=1");
  for (int64_t i = 0; OB_SUCC(ret) && i < ai_table_ids.count(); ++i) {
    ret = install_table(ai_table_ids.at(i), true);
  }
  return ret;
}

int AiColumnService::process_group()
{
  int ret = OB_SUCCESS;
  if (!GCTX.sql_proxy_) return OB_NOT_INIT;
  if (!has_ai_columns_.load()) return OB_ITER_END;
  // Idle polling is read-only; acquire a transactional lock only when work is due.
  // This also avoids repeatedly creating and rolling back empty transactions.
  {
    ObMySQLProxy::MySQLResult available;
    std::string sql = "SELECT 1 FROM " + std::string(TASKS)
        + " WHERE state IN(0,1,3,4,5) AND eligible_ts<=NOW(6) AND (lease_until IS NULL OR lease_until<NOW(6)) LIMIT 1";
    if (OB_FAIL(GCTX.sql_proxy_->read(available, sql.c_str()))) return ret;
    if (!available.get_result()) return OB_ERR_UNEXPECTED;
    if (OB_FAIL(available.get_result()->next())) return ret;
  }
  static std::atomic<uint64_t> sequence{0};
  const std::string owner = std::to_string(ObTimeUtility::current_time()) + "-" + std::to_string(sequence.fetch_add(1));
  uint64_t table_id = 0; std::string request;
  {
    ObMySQLTransaction transaction;
    if (OB_FAIL(transaction.start(GCTX.sql_proxy_))) return ret;
    ObMySQLProxy::MySQLResult result;
    std::string sql = "SELECT table_id,request_id FROM " + std::string(TASKS)
        + " WHERE state IN(0,1,3,4,5) AND eligible_ts<=NOW(6) AND (lease_until IS NULL OR lease_until<NOW(6))"
          " ORDER BY eligible_ts,table_id,request_id,column_id LIMIT 1 FOR UPDATE SKIP LOCKED";
    if (OB_FAIL(transaction.read(result, sql.c_str()))) {
    } else if (!result.get_result()) ret = OB_ERR_UNEXPECTED;
    else if (OB_FAIL(result.get_result()->next())) {
    } else {
      int64_t id = 0; ObString key;
      if (OB_FAIL(result.get_result()->get_int("table_id", id))) {
      } else if (OB_FAIL(result.get_result()->get_varchar("request_id", key))) {
      } else { table_id = id; request = str(key); }
    }
    // Release the result reference before issuing another statement on this transaction.
    result.reset();
    int64_t claimed = 0;
    if (OB_SUCC(ret)) ret = write_sql(transaction, "UPDATE " + std::string(TASKS) + " SET claim_id=" + ac::literal(owner)
        + ",lease_until=DATE_ADD(NOW(6),INTERVAL 300 SECOND) WHERE " + group_where(table_id, request)
        + " AND (lease_until IS NULL OR lease_until<NOW(6))", &claimed);
    if (OB_SUCC(ret) && claimed == 0) ret = OB_ITER_END;
    int end = transaction.end(OB_SUCC(ret)); if (OB_SUCC(ret)) ret = end;
    if (ret == OB_ITER_END) return OB_ITER_END;
    if (OB_FAIL(ret)) return ret;
  }
  struct ClaimScope {
    uint64_t table_id;
    const std::string &request, &owner;
    ~ClaimScope() {
      try {
        THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 5 * 1000 * 1000);
        if (GCTX.sql_proxy_) {
          const int code = write_sql(*GCTX.sql_proxy_, "UPDATE " + std::string(TASKS)
              + " SET claim_id=NULL,lease_until=NULL WHERE " + group_where(table_id, request)
              + " AND claim_id=" + ac::literal(owner));
          UNUSED(code);
        }
      } catch (...) { /* The durable lease remains recoverable if SQL is unavailable. */ }
    }
  } claim_scope{table_id, request, owner};
  ObSchemaGetterGuard guard;
  const ObTableSchema *table = nullptr;
  const ObDatabaseSchema *database = nullptr;
  if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) return ret;
  if (OB_FAIL(guard.get_table_schema(table_id, table))) return ret;
  if (!table || table->is_in_recyclebin()) return write_sql(*GCTX.sql_proxy_, "DELETE FROM " + std::string(TASKS) + " WHERE " + group_where(table_id, request));
  if (OB_FAIL(guard.get_database_schema(table->get_database_id(), database))) return ret;
  if (!database) return OB_ERR_UNEXPECTED;
  auto columns = columns_of(*table);
  std::vector<Task> tasks;
  if (OB_FAIL(read_tasks(*GCTX.sql_proxy_, table_id, request, tasks))) return ret;
  for (auto &task : tasks) {
    if (has_set_stop()) break;
    if (task.state == TASK_READY || task.state == TASK_FAILED || !task.eligible) continue;
    auto column = std::find_if(columns.begin(), columns.end(), [&](const Column &value) { return value.id == task.column_id; });
    int call = OB_SUCCESS;
    if (column == columns.end() || column->definition.definition_version != task.version) {
      return write_sql(*GCTX.sql_proxy_, "DELETE FROM " + std::string(TASKS) + " WHERE " + group_where(table_id, request));
    }
    const std::string identity = group_where(table_id, request) + " AND column_id=" + std::to_string(task.column_id) + " AND claim_id=" + ac::literal(owner);
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 120 * 1000 * 1000);
    if (OB_FAIL(write_sql(*GCTX.sql_proxy_, "UPDATE " + std::string(TASKS)
        + " SET lease_until=DATE_ADD(NOW(6),INTERVAL 300 SECOND) WHERE " + group_where(table_id, request)
        + " AND claim_id=" + ac::literal(owner)))) return ret;
    int64_t owned = 0;
    if (OB_FAIL(write_sql(*GCTX.sql_proxy_, "UPDATE " + std::string(TASKS) + " SET state=1,eligible_ts=NOW(6) WHERE " + identity, &owned))) return ret;
    if (!owned) return OB_SUCCESS;
    if (task.input_null) task.result_null = true;
    else { task.result_null = false; call = model_call(column->definition, task.input, task.result); }
    if (call == OB_SUCCESS && !task.result_null) call = validate_output(*table->get_column_schema(column->id), column->definition, task.result);
    std::string update;
    if (call == OB_SUCCESS) {
      task.state = TASK_READY;
      update = "state=4,result_text=" + (task.result_null ? std::string("NULL") : ac::literal(task.result)) + ",error_code=0";
    } else if (task.retries < column->definition.max_retries) {
      task.state = TASK_RETRY; ++task.retries;
      update = "state=3,retry_count=" + std::to_string(task.retries) + ",error_code=" + std::to_string(call)
          + ",eligible_ts=DATE_ADD(NOW(6),INTERVAL " + std::to_string(std::min<int64_t>(30, 2LL << std::min<int64_t>(task.retries - 1, 4))) + " SECOND)";
    } else {
      task.state = TASK_FAILED;
      update = "state=5,result_text=NULL,error_code=" + std::to_string(call);
    }
    if (OB_FAIL(write_sql(*GCTX.sql_proxy_, "UPDATE " + std::string(TASKS) + " SET " + update + " WHERE " + identity))) return ret;
  }
  if (has_set_stop()) return OB_CANCELED;
  tasks.clear();
  if (OB_FAIL(read_tasks(*GCTX.sql_proxy_, table_id, request, tasks))) return ret;
  if (tasks.empty()) return OB_SUCCESS;
  bool terminal = true;
  for (const auto &task : tasks) terminal &= task.state == TASK_READY || task.state == TASK_FAILED;
  if (!terminal) {
    // Terminal members wait until the next unfinished member is eligible, avoiding
    // a hot publication loop while preserving each failed call's retry backoff.
    const std::string where = group_where(table_id, request);
    ret = write_sql(*GCTX.sql_proxy_, "UPDATE " + std::string(TASKS)
        + " SET eligible_ts=DATE_ADD(NOW(6),INTERVAL 1 SECOND) WHERE " + where + " AND state IN(4,5)");
    if (OB_SUCC(ret)) ret = write_sql(*GCTX.sql_proxy_, "UPDATE " + std::string(TASKS)
        + " SET claim_id=NULL,lease_until=NULL WHERE " + where + " AND claim_id=" + ac::literal(owner));
    return ret;
  }
  std::string update = "UPDATE " + ac::identifier(str(database->get_database_name_str())) + "." + ac::identifier(str(table->get_table_name_str())) + " SET ";
  std::string predicate = "(" + tasks.front().rowkey + ")";
  bool first = true;
  for (const auto &task : tasks) {
    auto column = std::find_if(columns.begin(), columns.end(), [&](const Column &value) { return value.id == task.column_id; });
    if (column == columns.end() || column->definition.definition_version != task.version) return OB_SCHEMA_EAGAIN;
    if (!first) update += ","; first = false;
    update += ac::identifier(column->name) + "=";
    update += (task.state == TASK_FAILED || task.result_null) ? "NULL" : ac::literal(task.result);
    update += "," + ac::identifier(column->state) + "=" + std::to_string(task.state == TASK_READY ? 1 : 2);
    predicate += " AND " + ac::identifier(column->request) + "=" + ac::literal(request)
        + " AND " + ac::identifier(column->spec) + "=" + ac::literal(task.version);
    predicate += " AND EXISTS (SELECT 1 FROM oceanbase.__all_ai_column a WHERE a.table_id=" + std::to_string(table_id)
        + " AND a.column_id=" + std::to_string(task.column_id) + " AND a.definition_version=" + ac::literal(task.version) + ")";
  }
  update += " WHERE " + predicate;
  ObMySQLTransaction publish;
  if (OB_FAIL(publish.start(GCTX.sql_proxy_))) return ret;
  int64_t owned = 0;
  if (OB_FAIL(write_sql(publish, "UPDATE " + std::string(TASKS)
      + " SET lease_until=DATE_ADD(NOW(6),INTERVAL 300 SECOND) WHERE " + group_where(table_id, request)
      + " AND claim_id=" + ac::literal(owner), &owned))) {
  } else if (owned != 0 && owned != static_cast<int64_t>(tasks.size())) {
    // The internal SQL session can evaluate NOW(6) from a stale statement clock,
    // making the lease refresh a no-op (zero changed rows). That is not proof of
    // lost ownership: the base-row UPDATE re-checks request token and definition
    // version, and the task DELETE below is guarded by the claim ownership.
    ret = OB_EAGAIN;
  }
  if (OB_SUCC(ret) && table->is_table_with_hidden_pk_column()) {
    std::vector<Task> locked_tasks;
    if (OB_FAIL(read_tasks(publish, table_id, request, locked_tasks))) {
    } else if (locked_tasks.size() != tasks.size()) ret = OB_EAGAIN;
    else for (size_t i = 0; OB_SUCC(ret) && i < tasks.size(); ++i) {
      if (locked_tasks[i].rowkey != tasks[i].rowkey) ret = OB_EAGAIN;
    }
  }
  if (OB_SUCC(ret)) ret = write_sql(publish, update);
  if (OB_FAIL(ret)) {
    LOG_WARN("AI column publication failed", K(ret), K(table_id), "request", request.c_str());
  } else ret = write_sql(publish, "DELETE FROM " + std::string(TASKS) + " WHERE " + group_where(table_id, request) + " AND claim_id=" + ac::literal(owner));
  int end = publish.end(OB_SUCC(ret)); if (OB_SUCC(ret)) ret = end;
  if (OB_FAIL(ret)) {
    // Keep typed terminal results durable and retry publication without a model call.
    const int release_ret = write_sql(*GCTX.sql_proxy_, "UPDATE " + std::string(TASKS)
        + " SET claim_id=NULL,lease_until=NULL,eligible_ts=DATE_ADD(NOW(6),INTERVAL 1 SECOND) WHERE "
        + group_where(table_id, request) + " AND claim_id=" + ac::literal(owner));
    if (release_ret != OB_SUCCESS) LOG_WARN("cannot release AI publication claim", K(release_ret));
  }
  return ret;
}

void AiColumnService::run1()
{
  lib::set_thread_name("AiColumnWorker");
  if (get_thread_idx() == 0) {
    while (!has_set_stop()) {
      THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 120 * 1000 * 1000);
      int ret = OB_SUCCESS;
      try { ret = recover(); }
      catch (const std::bad_alloc &) { ret = OB_ALLOCATE_MEMORY_FAILED; }
      catch (const std::exception &) { ret = OB_ERR_UNEXPECTED; }
      if (ret == OB_SUCCESS) { recovered_.store(true); break; }
      ob_usleep(200000);
    }
  }
  while (!has_set_stop()) {
    if (!recovered_.load()) { ob_usleep(100000); continue; }
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 120 * 1000 * 1000);
    try {
      int ret = process_group();
      if (ret != OB_SUCCESS) {
        if (ret != OB_ITER_END) LOG_WARN("AI column dispatch failed", K(ret));
        ob_usleep(100000);
      }
    } catch (const std::exception &error) {
      int ret = OB_ERR_UNEXPECTED;
      LOG_WARN("AI column worker exception", "error", error.what()); ob_usleep(200000);
    }
  }
}
}}
