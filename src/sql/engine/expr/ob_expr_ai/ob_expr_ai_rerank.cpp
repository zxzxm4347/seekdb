/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_ai_rerank.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "lib/utility/utility.h"
#include "common/json_type/ob_json_common.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <vector>
 
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase 
{
namespace sql 
{

namespace
{
constexpr char RERANK_INDEX_KEY[] = "index";
constexpr char RERANK_SCORE_KEY[] = "relevance_score";

int get_json_score(ObIJsonBase *node, double &score)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_DATA;
  } else {
    switch (node->json_type()) {
      case ObJsonNodeType::J_DOUBLE:
      case ObJsonNodeType::J_ODOUBLE: {
        score = node->get_double();
        break;
      }
      case ObJsonNodeType::J_OFLOAT: {
        score = static_cast<double>(node->get_float());
        break;
      }
      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_OINT: {
        score = static_cast<double>(node->get_int());
        break;
      }
      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_OLONG: {
        score = static_cast<double>(node->get_uint());
        break;
      }
      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_ODECIMAL: {
        number::ObNumber decimal_val = node->get_decimal_data();
        ObScale scale = node->get_decimal_scale();
        char decimal_buf[512];
        int64_t pos = 0;
        if (OB_FAIL(decimal_val.format_v2(decimal_buf, sizeof(decimal_buf) - 1, pos, scale))) {
          ret = OB_INVALID_DATA;
        } else if (pos <= 0 || pos >= static_cast<int64_t>(sizeof(decimal_buf))) {
          ret = OB_INVALID_DATA;
        } else {
          decimal_buf[pos] = '\0';
          char *end_ptr = nullptr;
          errno = 0;
          score = std::strtod(decimal_buf, &end_ptr);
          if (end_ptr == decimal_buf || *end_ptr != '\0' || errno == ERANGE) {
            ret = OB_INVALID_DATA;
          }
        }
        break;
      }
      default: {
        ret = OB_INVALID_DATA;
        break;
      }
    }
    if (OB_SUCC(ret) && !std::isfinite(score)) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

int get_json_index(ObIJsonBase *node, int64_t &index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_DATA;
  } else if (ObJsonNodeType::J_INT == node->json_type()
             || ObJsonNodeType::J_OINT == node->json_type()) {
    index = node->get_int();
  } else if (ObJsonNodeType::J_UINT == node->json_type()
             || ObJsonNodeType::J_OLONG == node->json_type()) {
    const uint64_t unsigned_index = node->get_uint();
    if (unsigned_index > static_cast<uint64_t>(INT64_MAX)) {
      ret = OB_INVALID_DATA;
    } else {
      index = static_cast<int64_t>(unsigned_index);
    }
  } else {
    ret = OB_INVALID_DATA;
  }
  return ret;
}

struct RerankResultItem
{
  double score_;
  int64_t index_;
  ObJsonNode *node_;
};

// Validate the provider response (one object per document, unique integer index
// in range, finite numeric score) and sort it by score desc with the original
// index as the stable tie breaker.
int validate_and_sort_rerank_result(ObIAllocator &allocator,
                                    ObJsonArray *model_result_array,
                                    int64_t doc_count,
                                    ObJsonArray *&sorted_result)
{
  int ret = OB_SUCCESS;
  ObJsonArray *sorted_array = nullptr;
  std::vector<RerankResultItem> items;
  if (OB_ISNULL(model_result_array) || doc_count <= 0) {
    ret = OB_INVALID_DATA;
  } else if (static_cast<int64_t>(model_result_array->element_count()) != doc_count) {
    ret = OB_INVALID_DATA;
    LOG_WARN("ai_rerank, result count mismatch with document count", K(ret),
        K(model_result_array->element_count()), K(doc_count));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, sorted_array))) {
  } else {
    std::vector<bool> seen(static_cast<size_t>(doc_count), false);
    items.reserve(static_cast<size_t>(doc_count));
    for (int64_t i = 0; OB_SUCC(ret) && i < doc_count; i++) {
      ObJsonNode *node = model_result_array->get_value(i);
      ObJsonObject *node_obj = NULL;
      double score = 0.0;
      int64_t index = 0;
      if (OB_ISNULL(node) || node->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_DATA;
        LOG_WARN("ai_rerank, result element is not object", K(ret), K(i));
      } else if (OB_ISNULL(node_obj = static_cast<ObJsonObject *>(node))) {
        ret = OB_INVALID_DATA;
      } else if (OB_FAIL(get_json_score(node_obj->get_value(RERANK_SCORE_KEY), score))) {
      } else if (OB_FAIL(get_json_index(node_obj->get_value(RERANK_INDEX_KEY), index))) {
      } else if (index < 0 || index >= doc_count || seen[static_cast<size_t>(index)]) {
        ret = OB_INVALID_DATA;
        LOG_WARN("ai_rerank, result index out of range or duplicated", K(ret), K(index), K(doc_count));
      } else {
        seen[static_cast<size_t>(index)] = true;
        RerankResultItem item;
        item.score_ = score;
        item.index_ = index;
        item.node_ = node;
        items.push_back(item);
      }
    }
  }
  if (OB_SUCC(ret)) {
    std::stable_sort(items.begin(), items.end(),
        [](const RerankResultItem &lhs, const RerankResultItem &rhs) {
          if (lhs.score_ != rhs.score_) {
            return lhs.score_ > rhs.score_;
          }
          return lhs.index_ < rhs.index_;
        });
    for (size_t i = 0; OB_SUCC(ret) && i < items.size(); i++) {
      if (OB_FAIL(sorted_array->append(items.at(i).node_))) {
        LOG_WARN("failed to append sorted rerank result", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    sorted_result = sorted_array;
  }
  return ret;
}
} // anonymous namespace

ObExprAIRerank::ObExprAIRerank(common::ObIAllocator &alloc)
: ObFuncExprOperator(alloc, 
                    T_FUN_SYS_AI_RERANK, 
                    N_AI_RERANK, 
                    MORE_THAN_ZERO,
                    NOT_VALID_FOR_GENERATED_COL, 
                    NOT_ROW_DIMENSION) 
{
}

ObExprAIRerank::~ObExprAIRerank() 
{
}

int ObExprAIRerank::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *types_stack,
                                    int64_t param_num,
                                    common::ObExprTypeCtx &type_ctx) const {
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 3 || param_num > 4)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    if (types_stack[MODEL_IDX].get_type() != ObNullType
        && !ob_is_string_type(types_stack[MODEL_IDX].get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, model key must be string type");
    } else if (types_stack[QUERY_IDX].get_type() != ObNullType
               && !ob_is_string_type(types_stack[QUERY_IDX].get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, query must be string type");
    } else {
      if (types_stack[MODEL_IDX].get_type() == ObNullType
          || ob_is_string_tc(types_stack[MODEL_IDX].get_type())) {
        types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
        types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      } else if (types_stack[MODEL_IDX].get_collation_type() != CS_TYPE_BINARY) {
        types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
      if (types_stack[QUERY_IDX].get_type() == ObNullType
          || ob_is_string_tc(types_stack[QUERY_IDX].get_type())) {
        types_stack[QUERY_IDX].set_calc_type(ObVarcharType);
        types_stack[QUERY_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      } else if (types_stack[QUERY_IDX].get_collation_type() != CS_TYPE_BINARY) {
        types_stack[QUERY_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
    ObObjType in_type = types_stack[DOCUMENTS_IDX].get_type();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, DOCUMENTS_IDX, N_AI_RERANK))) {
    } else if (ob_is_string_type(in_type) && types_stack[DOCUMENTS_IDX].get_collation_type() != CS_TYPE_BINARY) {
      if (types_stack[DOCUMENTS_IDX].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[DOCUMENTS_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (param_num == 4) {
      ObObjType doc_key_type = types_stack[DOC_KEY_IDX].get_type();
      if (doc_key_type == ObNullType) {
      } else if (doc_key_type == ObJsonType) {
      } else if (ob_is_string_type(doc_key_type)) {
        if (types_stack[DOC_KEY_IDX].get_collation_type() != CS_TYPE_BINARY) {
          types_stack[DOC_KEY_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      } else {  
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, doc key must be string type");
      }
    }

    if (OB_SUCC(ret)) {
      type.set_json();
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
    }
  }
  return ret;
}

int ObExprAIRerank::eval_ai_rerank(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) 
{
  INIT_SUCC(ret);
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_query = nullptr;
  ObDatum *arg_documents = nullptr;
  ObDatum *arg_doc_key = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_query, arg_documents, arg_doc_key))) {
  } else if (arg_model_id->is_null() || arg_query->is_null() || arg_documents->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, model id or query or documents is null");
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_RERANK));
    ObAIFuncExprInfo *info = nullptr;
    share::ObAiModelEndpointInfo resolved_endpoint;
    const share::ObAiModelEndpointInfo *endpoint_info = &resolved_endpoint;
    query::ObIAiEndpointResolver *endpoint_resolver =
        ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
    ObString model_id;
    ObString query;
    ObString doc_key;
    ObArray<ObString> header_array;
    ObJsonArray *document_array = nullptr;
    ObJsonArray *result_array = nullptr;
    ObIJsonBase *j_base = nullptr;
    int64_t batch_size = 20; // max batch size for rerank
    bool is_null_result = false;
    bool has_doc_key = false;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, temp_allocator,
                *arg_model_id, expr.args_[MODEL_IDX]->datum_meta_,
                expr.args_[MODEL_IDX]->obj_meta_.has_lob_header(), model_id))) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, temp_allocator,
                       *arg_query, expr.args_[QUERY_IDX]->datum_meta_,
                       expr.args_[QUERY_IDX]->obj_meta_.has_lob_header(), query))) {
    } else if (model_id.empty() || query.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, model id or query is empty");
      res.set_null();
    } else if (expr.arg_cnt_ == 4 && OB_NOT_NULL(arg_doc_key) && !arg_doc_key->is_null()) {
      ObExpr *doc_key_expr = expr.args_[DOC_KEY_IDX];
      if (OB_ISNULL(doc_key_expr)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (doc_key_expr->datum_meta_.type_ == ObJsonType) {
        ObIJsonBase *doc_key_jbase = nullptr;
        bool doc_key_is_null = false;
        if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, DOC_KEY_IDX,
                    doc_key_jbase, doc_key_is_null))) {
        } else if (doc_key_is_null || OB_ISNULL(doc_key_jbase)) {
          has_doc_key = false;
        } else if (doc_key_jbase->json_type() != ObJsonNodeType::J_STRING) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, doc key must be string type");
        } else {
          doc_key = static_cast<ObJsonString *>(doc_key_jbase)->get_str();
          has_doc_key = true;
        }
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, temp_allocator,
                         *arg_doc_key, doc_key_expr->datum_meta_,
                         doc_key_expr->obj_meta_.has_lob_header(), doc_key))) {
      } else {
        has_doc_key = true;
      }
      if (OB_SUCC(ret) && has_doc_key && doc_key.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, doc key is empty");
        res.set_null();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, DOCUMENTS_IDX,
          j_base, is_null_result))) {
    } else if (is_null_result || OB_ISNULL(j_base)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, model id or query or documents is null");
    } else if (j_base->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, document_array is not array");
    } else if (OB_ISNULL(document_array = static_cast<ObJsonArray *>(j_base))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (document_array->element_count() == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, document_array is empty");
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < document_array->element_count(); i++) {
        ObJsonNode *doc_node = document_array->get_value(i);
        if (OB_ISNULL(doc_node)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (has_doc_key && doc_node->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, document_array element is not object");
        } else if (!has_doc_key && doc_node->json_type() != ObJsonNodeType::J_STRING) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, document_array element is not string");
        }
      }
    }
    
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(temp_allocator, model_id, info))) {
    } else if (OB_ISNULL(endpoint_resolver)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(endpoint_resolver->resolve_by_model_name(
                   model_id, temp_allocator, resolved_endpoint))) {
    } 

    if (OB_FAIL(ret)) {
    } else if (has_doc_key) {
      if (OB_FAIL(eval_ai_rerank_with_doc_key(expr, ctx, temp_allocator, model_id,
                  query, document_array, doc_key, *info, *endpoint_info, res))) {
      }
    } else {
      if (OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ObAIFuncUtils::check_info_type_rerank(info))) {
      } else if (OB_FAIL(ObAIFuncUtils::get_header(temp_allocator, *info, *endpoint_info, header_array))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(temp_allocator, result_array))) {
      }

      if (OB_SUCC(ret)) {
        ObString score_key(SCORE_KEY);
        int64_t end_idx = 0;
        ObJsonArray *compact_array = nullptr;
        ObJsonArray *batch_result_array = nullptr;
        ObJsonArray *batch_document_array = nullptr;
        for (int64_t i = 0; OB_SUCC(ret) && i < document_array->element_count(); i += batch_size) {
          end_idx = i + batch_size;
          if (end_idx > document_array->element_count()) {
            end_idx = document_array->element_count();
          }
          ObJsonArray *sorted_batch_result = nullptr;
          if (OB_FAIL(construct_batch_document_array(temp_allocator, document_array, i, end_idx, batch_document_array))) {
          } else if (OB_FAIL(inner_eval_ai_rerank(temp_allocator, *info, *endpoint_info, header_array, query, batch_document_array, batch_result_array))) {
          } else if (OB_FAIL(validate_and_sort_rerank_result(temp_allocator, batch_result_array,
                     batch_document_array->element_count(), sorted_batch_result))) {
          } else if (OB_FAIL(batch_result_add_base(temp_allocator, sorted_batch_result, i))) {
          } else if (OB_FAIL(compact_json_array_by_key(temp_allocator, result_array, sorted_batch_result, score_key, compact_array))) {
          } else {
            result_array = compact_array;
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObString raw_str;
        if (OB_FAIL(result_array->get_raw_binary(raw_str, &temp_allocator))) {
        } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_str))) {
        }
      }
    }
  }
  return ret;
}

int ObExprAIRerank::eval_ai_rerank_with_doc_key(const ObExpr &expr, ObEvalCtx &ctx, ObIAllocator &allocator, 
                                                ObString& model_id, ObString& query, ObJsonArray *document_array,
                                                ObString& doc_key, const ObAIFuncExprInfo &info,
                                                const share::ObAiModelEndpointInfo &endpoint_info, ObDatum &res)
{
  INIT_SUCC(ret);
  UNUSED(model_id);
  ObJsonArray *doc_array = nullptr;
  ObJsonArray *merged_result_array = nullptr;
  ObJsonArray *sorted_document_array = nullptr;
  const int64_t batch_size = 20;
  if (OB_FAIL(get_doc_array_from_documents_array_with_key(allocator, document_array, doc_key, doc_array))) {
  } else if (OB_FAIL(ObAIFuncUtils::check_info_type_rerank(&info))) {
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, merged_result_array))) {
  } else {
    ObAIFuncModel model(allocator, info, endpoint_info);
    ObString score_key(SCORE_KEY);
    for (int64_t i = 0; OB_SUCC(ret) && i < doc_array->element_count(); i += batch_size) {
      int64_t end_idx = i + batch_size;
      if (end_idx > doc_array->element_count()) {
        end_idx = doc_array->element_count();
      }
      ObJsonArray *batch_doc_array = nullptr;
      ObJsonArray *batch_result_array = nullptr;
      ObJsonArray *sorted_batch_result = nullptr;
      ObJsonArray *compact_array = nullptr;
      if (OB_FAIL(construct_batch_document_array(allocator, doc_array, i, end_idx, batch_doc_array))) {
      } else if (OB_FAIL(model.call_rerank(query, batch_doc_array, batch_result_array))) {
      } else if (OB_FAIL(validate_and_sort_rerank_result(allocator, batch_result_array,
                 batch_doc_array->element_count(), sorted_batch_result))) {
      } else if (OB_FAIL(batch_result_add_base(allocator, sorted_batch_result, i))) {
      } else if (OB_FAIL(compact_json_array_by_key(allocator, merged_result_array, sorted_batch_result, score_key, compact_array))) {
      } else {
        merged_result_array = compact_array;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sort_document_array_by_model_result(allocator, document_array, merged_result_array, sorted_document_array))) {
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObString raw_str;
    if (OB_FAIL(sorted_document_array->get_raw_binary(raw_str, &allocator))) {
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_str))) {
    }
  }
  return ret;
}

int ObExprAIRerank::get_doc_array_from_documents_array_with_key(ObIAllocator &allocator, ObJsonArray *document_object_array, ObString &doc_key, ObJsonArray *&doc_array)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(document_object_array)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (doc_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObJsonArray *doc_array_obj = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, doc_array_obj))) {
    } else {
      int64_t count = document_object_array->element_count();
      ObJsonNode *doc_node = nullptr;
      ObJsonNode *doc_value_node = nullptr;
      ObJsonObject *doc_obj = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        doc_node = document_object_array->get_value(i);
        if (OB_ISNULL(doc_node)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR (OB_INVALID_ARGUMENT, "ai_rerank, document_array element is null");
        } else if (doc_node->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR (OB_INVALID_ARGUMENT, "ai_rerank, document_array element is not object");
        } else {
          doc_obj = static_cast<ObJsonObject *>(doc_node);
          doc_value_node = doc_obj->get_value(doc_key);
          if (OB_ISNULL(doc_value_node)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR (OB_INVALID_ARGUMENT, "ai_rerank, the value for given doc key is null");
          } else if (doc_value_node->json_type() != ObJsonNodeType::J_STRING) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR (OB_INVALID_ARGUMENT, "ai_rerank, the value for given doc key is not string");
          } else if (OB_FAIL(doc_array_obj->append(doc_value_node))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      doc_array = doc_array_obj;
    }
  }
  return ret;
}

int ObExprAIRerank::sort_document_array_by_model_result(ObIAllocator &allocator, ObJsonArray *document_array, ObJsonArray *model_result_array, ObJsonArray *&sorted_document_array)
{
  INIT_SUCC(ret);
  ObJsonArray *sorted_document_array_obj = nullptr;
  ObJsonArray *sorted_result_array = nullptr;
  if (OB_ISNULL(document_array) || OB_ISNULL(model_result_array)) {
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(validate_and_sort_rerank_result(allocator, model_result_array,
             document_array->element_count(), sorted_result_array))) {
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, sorted_document_array_obj))) {
  } else {
    int64_t count = sorted_result_array->element_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObJsonObject *model_result_obj = static_cast<ObJsonObject *>(sorted_result_array->get_value(i));
      ObJsonNode *index_node = nullptr;
      ObJsonNode *score_node = nullptr;
      ObJsonObject *origin_obj = nullptr;
      ObJsonObject *new_obj = nullptr;
      int64_t index = 0;
      if (OB_ISNULL(model_result_obj) || model_result_obj->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_DATA;
      } else if (OB_ISNULL(index_node = model_result_obj->get_value(ObString(INDEX_KEY)))) {
        ret = OB_INVALID_DATA;
      } else if (OB_FAIL(get_json_index(index_node, index))) {
      } else if (index < 0 || index >= document_array->element_count()) {
        ret = OB_INVALID_DATA;
      } else if (OB_ISNULL(score_node = model_result_obj->get_value(ObString(SCORE_KEY)))) {
        ret = OB_INVALID_DATA;
      } else if (OB_ISNULL(origin_obj = static_cast<ObJsonObject *>(document_array->get_value(index)))) {
        ret = OB_INVALID_DATA;
      } else if (OB_ISNULL(new_obj = static_cast<ObJsonObject *>(origin_obj->clone(&allocator, false)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_NOT_NULL(new_obj->get_value(ObString(MODEL_SCORE_KEY)))
                 && OB_FAIL(new_obj->remove(ObString(MODEL_SCORE_KEY)))) {
      } else {
        ObJsonNode *cloned_score = score_node->clone(&allocator, false);
        if (OB_ISNULL(cloned_score)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(new_obj->add(MODEL_SCORE_KEY, cloned_score))) {
        } else if (OB_FAIL(sorted_document_array_obj->append(new_obj))) {
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    sorted_document_array = sorted_document_array_obj;
  }
  return ret;
}

int ObExprAIRerank::construct_batch_document_array(ObIAllocator &allocator, ObJsonArray *document_array, int64_t start_idx, int64_t end_idx, ObJsonArray *&batch_document_array)
{
  INIT_SUCC(ret);
  ObJsonArray *result_array = nullptr;
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, result_array))) {
  } else {
    for (int64_t i = start_idx; i < end_idx; i++) {
      if (OB_FAIL(result_array->append(document_array->get_value(i)))) {
      }
    }
  }
  if (OB_SUCC(ret)) {
    batch_document_array = result_array;
  }
  return ret;
}

int ObExprAIRerank::inner_eval_ai_rerank(ObIAllocator &allocator, 
                                          const ObAIFuncExprInfo &info, 
                                          const share::ObAiModelEndpointInfo &endpoint_info,
                                          ObArray<ObString> &header_array, 
                                          ObString &query, 
                                          ObJsonArray *document_array, 
                                          ObJsonArray *&result_array)
{
  INIT_SUCC(ret);
  UNUSED(header_array);
  ObAIFuncModel model(allocator, info, endpoint_info);
  if (OB_FAIL(model.call_rerank(query, document_array, result_array))) {
  }
  return ret;
}

int ObExprAIRerank::batch_result_add_base(ObIAllocator &allocator, ObJsonArray *array, int64_t start_idx)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(array)) {
    ret = OB_INVALID_DATA;
  } else {
    int64_t count = array->element_count();
    ObString index_key(INDEX_KEY);
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObJsonNode *elem = array->get_value(i);
      ObJsonObject *obj = nullptr;
      int64_t cur_index = 0;
      ObJsonInt *new_index_node = nullptr;
      if (OB_ISNULL(elem) || elem->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_DATA;
      } else if (OB_ISNULL(obj = static_cast<ObJsonObject *>(elem))) {
        ret = OB_INVALID_DATA;
      } else if (OB_FAIL(get_json_index(obj->get_value(index_key), cur_index))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, cur_index + start_idx, new_index_node))) {
      } else if (OB_FAIL(obj->remove(index_key))) {
      } else if (OB_FAIL(obj->add(index_key, new_index_node))) {
      }
    }
  }
  return ret;
}

int ObExprAIRerank::compact_json_array_by_key(ObIAllocator &allocator, ObJsonArray *array1, ObJsonArray *array2, ObString &score_key, ObJsonArray *&compact_array)
{
  INIT_SUCC(ret);
  ObJsonArray *result_array = nullptr;
  if (OB_ISNULL(array1) || OB_ISNULL(array2)) {
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, result_array))) {
  } else {
    int64_t count1 = array1->element_count();
    int64_t count2 = array2->element_count();
    ObString index_key(INDEX_KEY);
    int64_t i = 0;
    int64_t j = 0;
    while (OB_SUCC(ret) && i < count1 && j < count2) {
      ObJsonNode *node1 = array1->get_value(i);
      ObJsonNode *node2 = array2->get_value(j);
      ObJsonObject *obj1 = nullptr;
      ObJsonObject *obj2 = nullptr;
      double score1 = 0.0;
      double score2 = 0.0;
      int64_t idx1 = 0;
      int64_t idx2 = 0;
      if (OB_ISNULL(node1) || OB_ISNULL(node2)
          || node1->json_type() != ObJsonNodeType::J_OBJECT
          || node2->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_DATA;
      } else if (OB_FALSE_IT(obj1 = static_cast<ObJsonObject *>(node1))) {
      } else if (OB_FALSE_IT(obj2 = static_cast<ObJsonObject *>(node2))) {
      } else if (OB_FAIL(get_json_score(obj1->get_value(score_key), score1))) {
      } else if (OB_FAIL(get_json_score(obj2->get_value(score_key), score2))) {
      } else if (OB_FAIL(get_json_index(obj1->get_value(index_key), idx1))) {
      } else if (OB_FAIL(get_json_index(obj2->get_value(index_key), idx2))) {
      } else {
        const bool take_first = (score1 != score2) ? (score1 > score2) : (idx1 <= idx2);
        if (take_first) {
          if (OB_FAIL(result_array->append(obj1))) {
          }
          i++;
        } else {
          if (OB_FAIL(result_array->append(obj2))) {
          }
          j++;
        }
      }
    }
    while (OB_SUCC(ret) && i < count1) {
      if (OB_FAIL(result_array->append(array1->get_value(i)))) {
      }
      i++;
    }
    while (OB_SUCC(ret) && j < count2) {
      if (OB_FAIL(result_array->append(array2->get_value(j)))) {
      }
      j++;
    }
  }
  if (OB_SUCC(ret)) {
    compact_array = result_array;
  }
  return ret;
}

int ObExprAIRerank::construct_config_json(ObIAllocator &allocator, int64_t top_n, int64_t return_doc, ObJsonObject *&config_json)
{
  INIT_SUCC(ret);
  ObJsonObject *config_obj = nullptr;
  ObJsonInt *top_n_obj = nullptr;
  ObJsonBoolean *return_doc_obj = nullptr;
  bool return_doc_value = true;
  if (return_doc <= 0) {
    return_doc_value = false;
  }
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config_obj))) {
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_boolean(allocator, return_doc_value, return_doc_obj))) {
  } else if (OB_FAIL(config_obj->add("return_documents", return_doc_obj))) {
  } else if (top_n > 0) {
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, top_n, top_n_obj))) {
    } else if (OB_FAIL(config_obj->add("top_n", top_n_obj))) {
    }
  }
  if (OB_SUCC(ret)) {
    config_json = config_obj;
  }
  return ret;
}

int ObExprAIRerank::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const 
{
  int ret = OB_SUCCESS;
  // TODO: support schema version match in plan cache for ai func
  // const ObRawExpr *model_key = raw_expr.get_param_expr(0);
  // if (OB_NOT_NULL(model_key)
  //     && (model_key->is_static_scalar_const_expr() || model_key->is_const_expr())
  //     && model_key->get_expr_type() != T_OP_GET_USER_VAR &&
  //     OB_NOT_NULL(expr_cg_ctx.schema_guard_)) {
  //   ObIAllocator *allocator = expr_cg_ctx.allocator_;
  //   ObExecContext *exec_ctx = expr_cg_ctx.session_->get_cur_exec_ctx();
  //   bool got_data = false;
  //   ObObj const_data;
  //   ObAIFuncExprInfo *info = nullptr;
  //   if (OB_ISNULL(allocator)) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_WARN("allocator is null", K(ret));
  //   } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
  //                                                         model_key,
  //                                                         const_data,
  //                                                         got_data,
  //                                                         *allocator))) {
  //     LOG_WARN("failed to calc offset expr", K(ret));
  //   } else if (!got_data || const_data.is_null()) {
  //   } else {
  //     ObAIFuncExprInfo *info = OB_NEWx(ObAIFuncExprInfo, allocator, *allocator, T_FUN_SYS_AI_RERANK);
  //     if (OB_ISNULL(info)) {
  //       ret = OB_ALLOCATE_MEMORY_FAILED;
  //       LOG_WARN("failed to allocate memory for ai rerank info", K(ret));
  //     } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(*allocator, const_data.get_string(), *expr_cg_ctx.schema_guard_, info))) {
  //       LOG_WARN("failed to get ai func info", K(ret), K(const_data.get_string()));
  //     } else {
  //       rt_expr.extra_info_ = info;
  //     }
  //   }
  // }
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIRerank::eval_ai_rerank;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
