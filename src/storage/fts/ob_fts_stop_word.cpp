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

#include "object/ob_object.h"
#define USING_LOG_PREFIX STORAGE_FTS

#include "share/rc/ob_server_runtime.h"
#include "storage/fts/ob_fts_stop_word.h"
#include "storage/fts/ob_fts_parser_helper.h"
#include "storage/fts/ob_fts_parser_property.h"

namespace oceanbase
{
namespace storage
{

////////////////////////////////////////////////////////////////////////////////
// class ObStopWordChecker
ObStopWordChecker::~ObStopWordChecker()
{
  destroy();
}

int ObStopWordChecker::init()
{
  int ret = OB_SUCCESS;
  
  

  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(stopword_set_.create(DEFAULT_STOPWORD_BUCKET_NUM, "StopWordSet", "StopWordSet"))) {
  } else {
    ObObjMeta stop_meta;
    stop_meta.set_varchar();
    stop_meta.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);

    stopword_type_.set_meta(stop_meta);
    const int64_t stopword_count = sizeof(ob_stop_word_list) / sizeof(ob_stop_word_list[0]);
    for (int64_t i = 0; OB_SUCC(ret) && i < stopword_count; ++i) {
      ObFTWord stopword(STRLEN(ob_stop_word_list[i]), ob_stop_word_list[i], stopword_type_);
      if (OB_FAIL(stopword_set_.set_refactored(stopword))) {
      }
    }

    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }
  return ret;
}

void ObStopWordChecker::destroy()
{
  if (inited_) {
    stopword_set_.destroy();
    inited_ = false;
  }
}

int ObStopWordChecker::check_stopword(const ObFTWord &word, bool &is_stopword)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(word.empty())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const common::ObString src_str = word.get_word().get_string();
    const ObCollationType src_coll = word.get_collation_type();
    const ObCollationType dst_coll = stopword_type_.get_collation_type();
    if (ObCharset::charset_type_by_coll(src_coll) == CHARSET_UTF8MB4
        && (src_str.length() > FTS_STOP_WORD_MAX_LENGTH
            || static_cast<unsigned char>(src_str.ptr()[0]) >= 0x80)) {
      is_stopword = false;
    } else if (ObCharset::charset_type_by_coll(src_coll) == ObCharset::charset_type_by_coll(dst_coll)) {
      ObFTWord converted(src_str.length(), src_str.ptr(), stopword_type_);
      ret = stopword_set_.exist_refactored(converted);
      if (OB_HASH_NOT_EXIST == ret) {
        is_stopword = false;
        ret = OB_SUCCESS;
      } else if (OB_HASH_EXIST == ret) {
        is_stopword = true;
        ret = OB_SUCCESS;
      } else if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      common::ObArenaAllocator allocator(lib::ObMemAttr("ChkStopWord"));
      common::ObString cmp_str;
      if (OB_FAIL(common::ObCharset::charset_convert(
                                         allocator,
                                         src_str,
                                         src_coll,
                                         dst_coll,
                                         cmp_str))) {
      } else {
        ObFTWord converted(cmp_str.length(), cmp_str.ptr(), stopword_type_);
        ret = stopword_set_.exist_refactored(converted);
        if (OB_HASH_NOT_EXIST == ret) {
          is_stopword = false;
          ret = OB_SUCCESS;
        } else if (OB_HASH_EXIST == ret) {
          is_stopword = true;
          ret = OB_SUCCESS;
        } else if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// class ObAddWord
ObAddWord::ObAddWord(
    const ObFTParserProperty &property,
    const ObObjMeta &meta,
    const ObAddWordFlag &flag,
    common::ObIAllocator &allocator,
    ObFTWordMap &word_map)
  : word_meta_(meta),
    allocator_(allocator),
    word_map_(word_map),
    min_max_word_cnt_(0),
    non_stopword_cnt_(0),
    stopword_cnt_(0),
    min_token_size_(property.min_token_size_),
    max_token_size_(property.max_token_size_),
    flag_(flag)
{
}

int ObAddWord::process_word(
    const char *word,
    const int64_t word_len,
    const int64_t char_cnt,
    const int64_t word_freq)
{
  int ret = OB_SUCCESS;
  bool is_stopword = false;
  ObFTWord src_word(word_len, word, word_meta_);
  ObFTWord dst_word;
  if (OB_ISNULL(word) || OB_UNLIKELY(0 >= word_len || 0 >= char_cnt || 0 >= word_freq)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_min_max_word(char_cnt)) {
    ++min_max_word_cnt_;
  } else if (OB_FAIL(casedown_word(src_word, dst_word))) {
  } else if (OB_FAIL(check_stopword(dst_word, is_stopword))) {
  } else if (OB_UNLIKELY(is_stopword)) {
    ++stopword_cnt_;
  } else if (OB_FAIL(groupby_word(dst_word, word_freq))) {
  } else {
    non_stopword_cnt_ += word_freq;
  }
  return ret;
}

bool ObAddWord::is_min_max_word(const int64_t c_len) const
{
  return flag_.min_max_word() && (c_len < min_token_size_ || c_len > max_token_size_);
}

int ObAddWord::casedown_word(const ObFTWord &src, ObFTWord &dst)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(src.empty())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (flag_.casedown()) {
    const ObString src_str = src.get_word().get_string();
    const char *ptr = src_str.ptr();
    const int32_t len = src_str.length();
    const ObCollationType coll_type = word_meta_.get_collation_type();
    if (ObCharset::charset_type_by_coll(coll_type) == CHARSET_UTF8MB4) {
      bool all_caseless = true;
      bool all_ascii = true;
      bool has_upper_ascii = false;
      int32_t i = 0;
      while (i < len) {
        const unsigned char c = static_cast<unsigned char>(ptr[i]);
        if (c < 0x80) {
          if (c >= 'A' && c <= 'Z') {
            has_upper_ascii = true;
            all_caseless = false;
          }
          ++i;
        } else {
          all_ascii = false;
          // UTF-8 3-byte codepoints with lead byte 0xE3..0xE9 (U+3000..U+9FFF, CJK punctuation & ideographs)
          // have NULL unicase pages in ob_unicase_pages_default and ob_unicase_pages_unicode520,
          // so ob_casedn_utf8mb4 leaves them unchanged.
          if (c >= 0xE3 && c <= 0xE9 && i + 2 < len
              && (static_cast<unsigned char>(ptr[i + 1]) & 0xC0) == 0x80
              && (static_cast<unsigned char>(ptr[i + 2]) & 0xC0) == 0x80) {
            i += 3;
          } else {
            all_caseless = false;
            break;
          }
        }
      }
      if (all_caseless) {
        dst = src;
        return OB_SUCCESS;
      } else if (all_ascii && has_upper_ascii) {
        char *buf = static_cast<char *>(allocator_.alloc(len));
        if (OB_ISNULL(buf)) {
          return OB_ALLOCATE_MEMORY_FAILED;
        }
        for (int32_t j = 0; j < len; ++j) {
          const unsigned char c = static_cast<unsigned char>(ptr[j]);
          buf[j] = static_cast<char>((c >= 'A' && c <= 'Z') ? (c + ('a' - 'A')) : c);
        }
        dst = ObFTWord(len, buf, word_meta_);
        return OB_SUCCESS;
      }
    }
    ObString dst_str;
    if (OB_FAIL(ObCharset::tolower(coll_type, src_str, dst_str, allocator_))) {
    } else {
      dst = ObFTWord(dst_str.length(), dst_str.ptr(), word_meta_);
    }
  } else {
    dst = src;
  }
  return ret;
}

int ObAddWord::check_stopword(const ObFTWord &ft_word, bool &is_stopword)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ft_word.empty())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (flag_.stopword()) {
    ObStopWordChecker *stop_word_checker = ObFTParseData::instance().stop_word_checker();
    if (OB_ISNULL(stop_word_checker)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(stop_word_checker->check_stopword(ft_word, is_stopword))) {
    }
  }
  return ret;
}

int ObAddWord::groupby_word(const ObFTWord &word, const int64_t word_freq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(word.empty() || word_freq <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t *cnt_ptr = word_map_.get(word);
    if (nullptr != cnt_ptr) {
      if (flag_.groupby_word()) {
        *cnt_ptr += word_freq;
      }
    } else {
      ObString copied;
      if (OB_FAIL(ob_write_string(allocator_, word.get_word().get_string(), copied))) {
      } else {
        ObFTWord owned(copied.length(), copied.ptr(), word_meta_);
        const int64_t init_cnt = flag_.groupby_word() ? 1 : 1;
        if (OB_FAIL(word_map_.set_refactored(owned, init_cnt))) {
        }
      }
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
