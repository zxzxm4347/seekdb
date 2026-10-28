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

#ifndef OB_SPARSE_RETRIEVAL_UTIL_H_
#define OB_SPARSE_RETRIEVAL_UTIL_H_

#include "common/datum/ob_datum.h"
#include "share/datum/ob_datum_funcs.h"

namespace oceanbase
{
namespace storage
{

// Domain ids (doc ids) are usually varbinary.  Under binary collation null_first_cmp is
// "nulls first, then memcmp, then length", which is cheap enough to inline into the merge loops
// instead of dispatching through the collation function table for every comparison.
// 作者：FDE WorkMate
OB_INLINE bool is_bytewise_comparable_id(const ObObjType type, const ObCollationType cs_type)
{
  return ob_is_string_type(type) && CS_TYPE_BINARY == cs_type && !is_lob_storage(type);
}

OB_INLINE int bytewise_null_first_cmp(const ObDatum &l, const ObDatum &r)
{
  int cmp_ret = 0;
  if (OB_UNLIKELY(l.is_null() || r.is_null())) {
    cmp_ret = l.is_null() ? (r.is_null() ? 0 : -1) : 1;
  } else if (OB_LIKELY(8 == l.len_ && 8 == r.len_)) {
    uint64_t lv = 0;
    uint64_t rv = 0;
    MEMCPY(&lv, l.ptr_, sizeof(uint64_t));
    MEMCPY(&rv, r.ptr_, sizeof(uint64_t));
    lv = __builtin_bswap64(lv);
    rv = __builtin_bswap64(rv);
    cmp_ret = (lv < rv) ? -1 : ((lv > rv) ? 1 : 0);
  } else {
    const uint32_t len = MIN(l.len_, r.len_);
    const int c = 0 == len ? 0 : memcmp(l.ptr_, r.ptr_, len);
    cmp_ret = 0 != c ? c : static_cast<int>(l.len_) - static_cast<int>(r.len_);
  }
  return cmp_ret;
}

class ObDomainIdCmp
{
public:
  ObDomainIdCmp() : cmp_func_(nullptr), is_bytewise_(false) {}
  ~ObDomainIdCmp() {}
  inline int init(const ObObjMeta &obj_meta);
  inline int compare(const ObDatum &lhs, const ObDatum &rhs, int &cmp_ret) const;
  inline void reset() { cmp_func_ = nullptr; is_bytewise_ = false; }
private:
  common::ObDatumCmpFuncType cmp_func_;
  bool is_bytewise_;
  DISALLOW_COPY_AND_ASSIGN(ObDomainIdCmp);
};

int ObDomainIdCmp::init(const ObObjMeta &obj_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!obj_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,"invalid argument", K(ret), K(obj_meta));
  } else {
    common::ObDatumBasicFuncs *id_basic_funcs = ObDatumFuncs::get_basic_func(
      obj_meta.get_type(), obj_meta.get_collation_type());
    if (OB_ISNULL(id_basic_funcs)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN,"failed to get basic functions", K(ret), K(obj_meta));
    } else {
      cmp_func_ = id_basic_funcs->null_first_cmp_;
      is_bytewise_ = is_bytewise_comparable_id(obj_meta.get_type(), obj_meta.get_collation_type());
    }
  }
  return ret;
}

int ObDomainIdCmp::compare(const ObDatum &lhs, const ObDatum &rhs, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  const bool lmax = lhs.is_max();
  const bool lmin = lhs.is_min();
  const bool rmax = rhs.is_max();
  const bool rmin = rhs.is_min();
  if (OB_UNLIKELY(nullptr == cmp_func_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN,"not init", K(ret));
  } else if (lhs.is_ext() || rhs.is_ext()) {
    if (OB_UNLIKELY(!(lmax || lmin || rmax || rmin))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN,"unexpected ext datum", K(ret), K(lhs), K(rhs));
    } else if (lmax || rmax) {
      cmp_ret = lmax - rmax;
    } else if (lmin || rmin) {
      cmp_ret = -(lmin - rmin);
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (is_bytewise_) {
    cmp_ret = bytewise_null_first_cmp(lhs, rhs);
  } else {
    ret = cmp_func_(lhs, rhs, cmp_ret, nullptr);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

#endif
