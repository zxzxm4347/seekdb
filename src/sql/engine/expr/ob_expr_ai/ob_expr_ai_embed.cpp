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
#include "ob_expr_ai_embed.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase 
{
namespace sql 
{
ObExprAIEmbed::ObExprAIEmbed(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, 
                        T_FUN_SYS_AI_EMBED, 
                        N_AI_EMBED, 
                        MORE_THAN_ZERO,
                        NOT_VALID_FOR_GENERATED_COL, 
                        NOT_ROW_DIMENSION) 
{
}

ObExprAIEmbed::~ObExprAIEmbed() 
{
}

int ObExprAIEmbed::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types_stack,
                                     int64_t param_num,
                                     common::ObExprTypeCtx &type_ctx) const 
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3 || param_num < 2)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (ob_is_text_tc(types_stack[CONTENT_IDX].get_type())) {
      if (types_stack[CONTENT_IDX].get_collation_type() != CS_TYPE_BINARY) {
        types_stack[CONTENT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    } else {
      types_stack[CONTENT_IDX].set_calc_type(ObVarcharType);
      types_stack[CONTENT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
    if (param_num == 3) {
      if (types_stack[DIM_IDX].get_type() == ObNullType) {
        // NULL dimension is treated as omitted (model default dimension).
      } else if (ob_is_integer_type(types_stack[DIM_IDX].get_type())) {
        types_stack[DIM_IDX].set_calc_type(ObIntType);
        types_stack[DIM_IDX].set_precision(10);
        types_stack[DIM_IDX].set_scale(0);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be an integer, not a decimal or float");
      }
    }
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) 
{
  INIT_SUCC(ret);
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_content = nullptr;
  ObDatum *arg_dim = nullptr;
  if (expr.arg_cnt_ == 3 ? OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content, arg_dim))
                         : OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content))) {
  } else if (arg_content->is_null()) {
    // null input propagates to a null result, no request is sent.
    res.set_null();
  } else if (arg_model_id->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or content is null");
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_EMBED));
    ObAIFuncExprInfo *info = nullptr;
    share::ObAiModelEndpointInfo resolved_endpoint;
    const share::ObAiModelEndpointInfo *endpoint_info = &resolved_endpoint;
    query::ObIAiEndpointResolver *endpoint_resolver =
        ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
    ObString model_id = arg_model_id->get_string();
    ObString content;
    if (ob_is_text_tc(expr.args_[CONTENT_IDX]->datum_meta_.type_)) {
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, temp_allocator,
                  *arg_content, expr.args_[CONTENT_IDX]->datum_meta_,
                  expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(), content))) {
      }
    } else {
      content = arg_content->get_string();
    }
    if (OB_FAIL(ret)) {
    } else if (model_id.empty() || content.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or input is empty");
      res.set_null();
    }
    int64_t dim = 0;
    ObJsonInt *dim_json = nullptr;
    ObJsonObject *config = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(arg_dim) && !arg_dim->is_null()) {
      dim = arg_dim->get_int();
      if (dim <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be a positive integer");
        res.set_null();
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(temp_allocator, config))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(temp_allocator, dim, dim_json))) {
      } else if (OB_FAIL(config->add("dimensions", dim_json))) {
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(temp_allocator, model_id, info))) {
    } else if (OB_ISNULL(endpoint_resolver)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(endpoint_resolver->resolve_by_model_name(
                   model_id, temp_allocator, resolved_endpoint))) {
    } else {
      ObAIFuncModel model(temp_allocator, *info, *endpoint_info);
      ObString result;
      if (OB_FAIL(model.call_dense_embedding(content, config, result))) {
      } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, result))) {
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::build_embed_dimension_config(common::ObIAllocator &allocator, const int64_t dim,
                                                common::ObJsonObject *&config)
{
  int ret = OB_SUCCESS;
  config = nullptr;
  if (dim > 0) {
    ObJsonObject *dim_config = nullptr;
    ObJsonInt *dim_json = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, dim_config))) {
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, dim, dim_json))) {
    } else if (OB_FAIL(dim_config->add("dimensions", dim_json))) {
    } else {
      config = dim_config;
    }
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed_one_row(const ObExpr &expr, ObEvalCtx &ctx,
                                         common::ObIAllocator &allocator,
                                         common::ObString &model_id, common::ObString &content,
                                         const bool dim_valid, const int64_t dim, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObAIFuncExprInfo *info = nullptr;
  share::ObAiModelEndpointInfo resolved_endpoint;
  const share::ObAiModelEndpointInfo *endpoint_info = &resolved_endpoint;
  query::ObIAiEndpointResolver *endpoint_resolver =
      ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
  ObJsonObject *config = nullptr;
  if (OB_FAIL(build_embed_dimension_config(allocator, dim_valid ? dim : 0, config))) {
  } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, model_id, info))) {
  } else if (OB_ISNULL(endpoint_resolver)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(endpoint_resolver->resolve_by_model_name(
                 model_id, allocator, resolved_endpoint))) {
  } else {
    ObAIFuncModel model(allocator, *info, *endpoint_info);
    ObString result;
    if (OB_FAIL(model.call_dense_embedding(content, config, result))) {
    } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, result))) {
    }
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed_batch(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       const ObBitVector &skip,
                                       const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector model_datums;
  ObDatumVector content_datums;
  ObDatumVector dim_datums;
  ObDatumVector res_datums = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_EMBED));
  const bool has_dim = expr.arg_cnt_ > DIM_IDX;
  if (OB_FAIL(expr.eval_batch_param_value(ctx, skip, batch_size,
              model_datums, content_datums, dim_datums))) {
  } else {
    struct EmbedBatchGroup
    {
      ObString model_id_;
      int64_t dimension_;
      ObArray<ObString> contents_;
      ObArray<int64_t> row_indexes_;
      EmbedBatchGroup() : model_id_(), dimension_(0), contents_(), row_indexes_() {}
    };
    ObArray<EmbedBatchGroup *> groups;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      ObDatum *model_datum = model_datums.at(i);
      ObDatum *content_datum = content_datums.at(i);
      if (OB_ISNULL(model_datum) || OB_ISNULL(content_datum)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (content_datum->is_null()) {
        eval_flags.set(i);
        res_datums.at(i)->set_null();
      } else if (model_datum->is_null()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or content is null");
      } else {
        ObString model_id = model_datum->get_string();
        ObString content;
        int64_t dim = 0;
        bool dim_valid = false;
        if (ob_is_text_tc(expr.args_[CONTENT_IDX]->datum_meta_.type_)) {
          ObEvalCtx::BatchInfoScopeGuard lob_guard(ctx);
          lob_guard.set_batch_idx(i);
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, temp_allocator,
                      *content_datum, expr.args_[CONTENT_IDX]->datum_meta_,
                      expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(), content))) {
          }
        } else {
          content = content_datum->get_string();
        }
        if (OB_FAIL(ret)) {
        } else if (has_dim) {
          ObDatum *dim_datum = dim_datums.at(i);
          if (OB_NOT_NULL(dim_datum) && !dim_datum->is_null()) {
            dim = dim_datum->get_int();
            dim_valid = true;
            if (dim <= 0) {
              ret = OB_INVALID_ARGUMENT;
              LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                  "ai_embed, dimension parameter must be a positive integer");
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (model_id.empty() || content.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or input is empty");
        } else {
          const int64_t effective_dim = dim_valid ? dim : 0;
          EmbedBatchGroup *target_group = nullptr;
          for (int64_t g = 0; g < groups.count(); ++g) {
            if (groups.at(g)->model_id_ == model_id && groups.at(g)->dimension_ == effective_dim) {
              target_group = groups.at(g);
              break;
            }
          }
          if (OB_ISNULL(target_group)) {
            if (OB_ISNULL(target_group = OB_NEWx(EmbedBatchGroup, &temp_allocator))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              target_group->model_id_ = model_id;
              target_group->dimension_ = effective_dim;
              if (OB_FAIL(groups.push_back(target_group))) {
                target_group->~EmbedBatchGroup();
                target_group = nullptr;
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(target_group->contents_.push_back(content))) {
            } else if (OB_FAIL(target_group->row_indexes_.push_back(i))) {
            }
          }
        }
      }
    }
    query::ObIAiEndpointResolver *endpoint_resolver =
        ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
    for (int64_t g = 0; OB_SUCC(ret) && g < groups.count(); ++g) {
      EmbedBatchGroup *grp = groups.at(g);
      ObAIFuncExprInfo *info = nullptr;
      share::ObAiModelEndpointInfo resolved_endpoint;
      const share::ObAiModelEndpointInfo *endpoint_info = &resolved_endpoint;
      ObJsonObject *config = nullptr;
      if (OB_ISNULL(grp) || 0 == grp->contents_.count()) {
      } else if (OB_FAIL(build_embed_dimension_config(temp_allocator, grp->dimension_, config))) {
      } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(temp_allocator, grp->model_id_, info))) {
      } else if (OB_ISNULL(endpoint_resolver)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(endpoint_resolver->resolve_by_model_name(
                     grp->model_id_, temp_allocator, resolved_endpoint))) {
      } else {
        ObArray<ObString> results;
        ObAIFuncModel model(temp_allocator, *info, *endpoint_info);
        if (OB_FAIL(model.call_dense_embedding_vector_v2(grp->contents_, config, results))) {
        } else if (results.count() != grp->row_indexes_.count()) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          for (int64_t k = 0; OB_SUCC(ret) && k < grp->row_indexes_.count(); ++k) {
            const int64_t row = grp->row_indexes_.at(k);
            ObString result_str = results.at(k);
            ObEvalCtx::BatchInfoScopeGuard guard(ctx);
            guard.set_batch_idx(row);
            if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, *res_datums.at(row), result_str))) {
            } else {
              eval_flags.set(row);
            }
          }
        }
      }
    }
    for (int64_t g = 0; g < groups.count(); ++g) {
      if (OB_NOT_NULL(groups.at(g))) {
        groups.at(g)->~EmbedBatchGroup();
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::cg_expr(ObExprCGCtx &expr_cg_ctx, 
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
  //   ObObj const_data;
  //   bool got_data = false;
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
  //   } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(*allocator, const_data.get_string(), *expr_cg_ctx.schema_guard_, info))) {
  //     LOG_WARN("failed to get ai func info", K(ret), K(const_data.get_string()));
  //   } else {
  //     rt_expr.extra_info_ = info;
  //   }
  // }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIEmbed::eval_ai_embed;
    rt_expr.eval_batch_func_ = ObExprAIEmbed::eval_ai_embed_batch;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
