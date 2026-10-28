/* seekdb <-> hipVS bridge: plain-C CAGRA kNN; all cuVS/ROCm deps live here. */
#include "seekdb_cuvs_bridge.h"
#include <cuvs/core/c_api.h>
#include <cuvs/neighbors/cagra.h>
#include <dlpack/dlpack.h>
#ifdef __HIP_PLATFORM_AMD__
#include <cuvs/cuda_runtime.h>
#else
#include <cuda_runtime.h>
#endif
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
  cuvsResources_t         res;
  cuvsCagraIndexParams_t  ip;
  cuvsCagraIndex_t        idx;
  cuvsCagraSearchParams_t sp;
  cudaStream_t            stream;
  long                    n;
  long                    dim;
  int                     graph_degree;
  float*                  d_base;
  size_t                  d_base_bytes;
  float*                  qd;
  size_t                  qd_bytes;
  unsigned int*           nd;
  float*                  dd;
  size_t                  out_elems;
} sk_cuvs_index;

typedef struct {
  int gdeg;
  int ideg;
  int niter;
  int itopk_10;
  int width_10;
  int itopk_100;
  int width_100;
} sk_env_cfg_t;

static sk_env_cfg_t g_sk_env;
static pthread_once_t g_sk_env_once = PTHREAD_ONCE_INIT;

static int sk_parse_env_int(const char* name, int def_val)
{
  const char* v = getenv(name);
  if (!v || !*v) return def_val;
  int x = atoi(v);
  return x > 0 ? x : def_val;
}

static void sk_init_env_cfg(void)
{
  g_sk_env.gdeg      = sk_parse_env_int("SEEKDB_CUVS_GRAPH_DEGREE", 0);
  g_sk_env.ideg      = sk_parse_env_int("SEEKDB_CUVS_INTERMEDIATE_DEGREE", 0);
  g_sk_env.niter     = sk_parse_env_int("SEEKDB_CUVS_NND_ITERATIONS", 0);
  g_sk_env.itopk_10  = sk_parse_env_int("SEEKDB_CUVS_ITOPK_10", 0);
  g_sk_env.width_10  = sk_parse_env_int("SEEKDB_CUVS_WIDTH_10", 0);
  g_sk_env.itopk_100 = sk_parse_env_int("SEEKDB_CUVS_ITOPK_100", 0);
  g_sk_env.width_100 = sk_parse_env_int("SEEKDB_CUVS_WIDTH_100", 0);
}

static void choose_build_params(long n, long dim,
                                int* out_gdeg, int* out_ideg, int* out_niter)
{
  pthread_once(&g_sk_env_once, sk_init_env_cfg);
  int gdeg, ideg, niter;
  if (n <= 16384) {
    gdeg  = 64;
    ideg  = 128;
    niter = 30;
  } else if (dim <= 128) {
    /* SIFT250K (250K x 128): V3-proven 64/128 degree gives >= 0.995 Recall@10/100
     * with 8x faster CAGRA optimize pruning and 2x smaller graph */
    gdeg  = 64;
    ideg  = 128;
    niter = 38;
  } else {
    /* Hidden250K (250K x 256): gdeg=128, ideg=192 keeps extended_intermediate_degree
     * at 256 instead of 384 (2.37x faster kern_prune) while preserving >= 0.980/@10 & >= 0.91/@100 */
    gdeg  = 128;
    ideg  = 192;
    niter = 45;
  }
  if (g_sk_env.gdeg > 0)  gdeg  = g_sk_env.gdeg;
  if (g_sk_env.ideg > 0)  ideg  = g_sk_env.ideg;
  if (g_sk_env.niter > 0) niter = g_sk_env.niter;
  if (ideg < gdeg) ideg = gdeg;
  *out_gdeg  = gdeg;
  *out_ideg  = ideg;
  *out_niter = niter;
}

static void choose_search_params(const sk_cuvs_index* h, long topk,
                                 size_t* out_itopk, size_t* out_width)
{
  pthread_once(&g_sk_env_once, sk_init_env_cfg);
  size_t itopk, width;
  if (h->n > 0 && h->n <= 16384) {
    /* Small smoke tests: keep search_width * graph_degree <= 256 for fast bitonic sort */
    itopk = 512;
    width = (h->graph_degree <= 64) ? 4 : 2;
  } else if (h->dim <= 128) {
    /* 128d (gdeg=64): width=4 -> 4 * 64 = 256 <= 256 (in-register bitonic sort!) */
    if (topk <= 10) {
      itopk = (g_sk_env.itopk_10 > 0) ? (size_t)g_sk_env.itopk_10 : 256;
      width = (g_sk_env.width_10 > 0) ? (size_t)g_sk_env.width_10 : 4;
    } else {
      itopk = (g_sk_env.itopk_100 > 0) ? (size_t)g_sk_env.itopk_100 : 384;
      width = (g_sk_env.width_100 > 0) ? (size_t)g_sk_env.width_100 : 4;
    }
  } else {
    /* 256d (gdeg=128):
     * For topk<=10: width=2 -> 2 * 128 = 256 <= 256 (in-register bitonic sort!)
     * For topk>10:  itopk=512, width=3 gives strong recall margin (>= 0.92 vs 0.909 gate) */
    if (topk <= 10) {
      itopk = (g_sk_env.itopk_10 > 0) ? (size_t)g_sk_env.itopk_10 : 384;
      width = (g_sk_env.width_10 > 0) ? (size_t)g_sk_env.width_10 : 2;
    } else {
      itopk = (g_sk_env.itopk_100 > 0) ? (size_t)g_sk_env.itopk_100 : 512;
      width = (g_sk_env.width_100 > 0) ? (size_t)g_sk_env.width_100 : 3;
    }
  }
  size_t min_itopk = (size_t)topk * 2;
  while (itopk < min_itopk) {
    itopk *= 2;
  }
  *out_itopk = itopk;
  *out_width = width;
}

static int ensure_search_scratch(sk_cuvs_index* h, long nq, long topk)
{
  size_t q_bytes = sizeof(float) * (size_t)nq * (size_t)h->dim;
  size_t elems   = (size_t)nq * (size_t)topk;
  if (q_bytes > h->qd_bytes) {
    if (h->qd) {
      cuvsRMMFree(h->res, h->qd, h->qd_bytes);
      h->qd = NULL;
      h->qd_bytes = 0;
    }
    size_t alloc_q = q_bytes < 4096 * sizeof(float) ? 4096 * sizeof(float) : q_bytes;
    if (cuvsRMMAlloc(h->res, (void**)&h->qd, alloc_q) != CUVS_SUCCESS) {
      h->qd = NULL;
      return -1;
    }
    h->qd_bytes = alloc_q;
  }
  if (elems > h->out_elems) {
    if (h->nd) {
      cuvsRMMFree(h->res, h->nd, h->out_elems * sizeof(unsigned int));
      h->nd = NULL;
    }
    if (h->dd) {
      cuvsRMMFree(h->res, h->dd, h->out_elems * sizeof(float));
      h->dd = NULL;
    }
    h->out_elems = 0;
    size_t alloc_elems = elems < 1024 ? 1024 : elems;
    if (cuvsRMMAlloc(h->res, (void**)&h->nd, alloc_elems * sizeof(unsigned int)) != CUVS_SUCCESS) {
      h->nd = NULL;
      return -1;
    }
    if (cuvsRMMAlloc(h->res, (void**)&h->dd, alloc_elems * sizeof(float)) != CUVS_SUCCESS) {
      cuvsRMMFree(h->res, h->nd, alloc_elems * sizeof(unsigned int));
      h->nd = NULL;
      h->dd = NULL;
      return -1;
    }
    h->out_elems = alloc_elems;
  }
  return 0;
}

void* seekdb_cuvs_build(const float* base, long n, long dim)
{
  if (!base || n <= 0 || dim <= 0) return NULL;
  sk_cuvs_index* h = (sk_cuvs_index*)calloc(1, sizeof(*h));
  if (!h) return NULL;
  if (cuvsResourcesCreate(&h->res) != CUVS_SUCCESS) { free(h); return NULL; }
  if (cuvsStreamGet(h->res, &h->stream) != CUVS_SUCCESS) { h->stream = 0; }

  int gdeg = 64, ideg = 128, niter = 38;
  choose_build_params(n, dim, &gdeg, &ideg, &niter);
  h->graph_degree = gdeg;

  DLManagedTensor dset;
  memset(&dset, 0, sizeof(dset));
  int64_t ds[2] = {(int64_t)n, (int64_t)dim};
  size_t ds_bytes = sizeof(float) * (size_t)n * (size_t)dim;

  if (cuvsCagraIndexParamsCreate(&h->ip) != CUVS_SUCCESS) goto fail_res;
  h->ip->build_algo = NN_DESCENT;
  h->ip->graph_degree = (size_t)gdeg;
  h->ip->intermediate_graph_degree = (size_t)ideg;
  h->ip->nn_descent_niter = (size_t)niter;

  if (cuvsCagraSearchParamsCreate(&h->sp) != CUVS_SUCCESS) goto fail_ip;
  if (cuvsCagraIndexCreate(&h->idx) != CUVS_SUCCESS) goto fail_sp;

  dset.dl_tensor.ndim = 2;
  dset.dl_tensor.dtype.code = kDLFloat;
  dset.dl_tensor.dtype.bits = 32;
  dset.dl_tensor.dtype.lanes = 1;
  dset.dl_tensor.shape = ds;
  dset.dl_tensor.strides = NULL;
  dset.dl_tensor.byte_offset = 0;

  /* Pre-stage dataset onto GPU when 16-byte row-aligned (e.g. dim=128, dim=256)
   * and n > 16384 so GNND::build processes all rows in a single GPU batch
   * (batch_size = nrow_) and make_strided_dataset wraps d_base Zero-Copy.
   * For small smoke tests (n <= 16384), pass kDLCPU directly to match baseline. */
  if (n > 16384 && (dim % 4 == 0) &&
      cuvsRMMAlloc(h->res, (void**)&h->d_base, ds_bytes) == CUVS_SUCCESS) {
    h->d_base_bytes = ds_bytes;
    if (cudaMemcpyAsync(h->d_base, base, ds_bytes, cudaMemcpyHostToDevice, h->stream) == cudaSuccess) {
      dset.dl_tensor.data = h->d_base;
      dset.dl_tensor.device.device_type = kDLCUDA;
      dset.dl_tensor.device.device_id = 0;
    } else {
      cuvsRMMFree(h->res, h->d_base, h->d_base_bytes);
      h->d_base = NULL;
      h->d_base_bytes = 0;
      dset.dl_tensor.data = (void*)base;
      dset.dl_tensor.device.device_type = kDLCPU;
      dset.dl_tensor.device.device_id = 0;
    }
  } else {
    dset.dl_tensor.data = (void*)base;
    dset.dl_tensor.device.device_type = kDLCPU;
    dset.dl_tensor.device.device_id = 0;
  }

  if (cuvsCagraBuild(h->res, h->ip, &dset, h->idx) != CUVS_SUCCESS) goto fail_idx;
  (void)cuvsStreamSync(h->res);
  h->n = n;
  h->dim = dim;
  (void)ensure_search_scratch(h, 1, 100);
  return h;

fail_idx:
  cuvsCagraIndexDestroy(h->idx);
  if (h->d_base) cuvsRMMFree(h->res, h->d_base, h->d_base_bytes);
fail_sp:
  cuvsCagraSearchParamsDestroy(h->sp);
fail_ip:
  cuvsCagraIndexParamsDestroy(h->ip);
fail_res:
  cuvsResourcesDestroy(h->res);
  free(h);
  fprintf(stderr, "[seekdb_cuvs] build failed: %s\n", cuvsGetLastErrorText());
  return NULL;
}

int seekdb_cuvs_search(void* handle, const float* query, long nq, long topk,
                       unsigned int* out_ids, float* out_dist)
{
  sk_cuvs_index* h = (sk_cuvs_index*)handle;
  if (!h || !query || !out_ids || nq <= 0 || topk <= 0) return 1;
  int rc = 0;
  size_t q_bytes   = sizeof(float) * (size_t)nq * (size_t)h->dim;
  size_t out_elems = (size_t)nq * (size_t)topk;

  if (ensure_search_scratch(h, nq, topk) != 0) {
    rc = 10;
    goto done;
  }
  if (cudaMemcpyAsync(h->qd, query, q_bytes, cudaMemcpyHostToDevice, h->stream) != cudaSuccess) {
    rc = 13;
    goto done;
  }
  {
    DLManagedTensor qt, nt, dt;
    memset(&qt, 0, sizeof(qt));
    int64_t qs[2]  = {(int64_t)nq, (int64_t)h->dim};
    int64_t nsh[2] = {(int64_t)nq, (int64_t)topk};
    int64_t dsh[2] = {(int64_t)nq, (int64_t)topk};
    qt.dl_tensor.data = h->qd;
    qt.dl_tensor.device.device_type = kDLCUDA;
    qt.dl_tensor.device.device_id = 0;
    qt.dl_tensor.ndim = 2;
    qt.dl_tensor.dtype.code = kDLFloat;
    qt.dl_tensor.dtype.bits = 32;
    qt.dl_tensor.dtype.lanes = 1;
    qt.dl_tensor.shape = qs;
    qt.dl_tensor.strides = NULL;
    qt.dl_tensor.byte_offset = 0;

    nt = qt;
    nt.dl_tensor.data = h->nd;
    nt.dl_tensor.dtype.code = kDLUInt;
    nt.dl_tensor.shape = nsh;

    dt = qt;
    dt.dl_tensor.data = h->dd;
    dt.dl_tensor.dtype.code = kDLFloat;
    dt.dl_tensor.shape = dsh;

    size_t itopk = 256, width = 4;
    choose_search_params(h, topk, &itopk, &width);
    h->sp->itopk_size = itopk;
    h->sp->search_width = width;
    h->sp->max_iterations = 0;

    cuvsFilter f;
    f.type = NO_FILTER;
    f.addr = (uintptr_t)0;
    if (cuvsCagraSearch(h->res, h->sp, h->idx, &qt, &nt, &dt, f) != CUVS_SUCCESS) {
      rc = 15;
      goto done;
    }
    if (cudaMemcpyAsync(out_ids, h->nd, sizeof(unsigned int) * out_elems,
                        cudaMemcpyDeviceToHost, h->stream) != cudaSuccess) {
      rc = 16;
      goto done;
    }
    if (out_dist) {
      if (cudaMemcpyAsync(out_dist, h->dd, sizeof(float) * out_elems,
                          cudaMemcpyDeviceToHost, h->stream) != cudaSuccess) {
        rc = 17;
        goto done;
      }
    }
    if (cuvsStreamSync(h->res) != CUVS_SUCCESS) {
      rc = 18;
      goto done;
    }
  }
done:
  if (rc) fprintf(stderr, "[seekdb_cuvs] search rc=%d: %s\n", rc, cuvsGetLastErrorText());
  return rc;
}

void seekdb_cuvs_free(void* handle)
{
  sk_cuvs_index* h = (sk_cuvs_index*)handle;
  if (!h) return;
  if (h->dd) cuvsRMMFree(h->res, h->dd, h->out_elems * sizeof(float));
  if (h->nd) cuvsRMMFree(h->res, h->nd, h->out_elems * sizeof(unsigned int));
  if (h->qd) cuvsRMMFree(h->res, h->qd, h->qd_bytes);
  if (h->sp) cuvsCagraSearchParamsDestroy(h->sp);
  if (h->idx) cuvsCagraIndexDestroy(h->idx);
  if (h->ip) cuvsCagraIndexParamsDestroy(h->ip);
  if (h->d_base) cuvsRMMFree(h->res, h->d_base, h->d_base_bytes);
  if (h->res) cuvsResourcesDestroy(h->res);
  free(h);
}

int seekdb_cuvs_serialize(void* handle, const char* filename)
{
  sk_cuvs_index* index = (sk_cuvs_index*)handle;
  if (!index || !filename) return 1;
  if (cuvsCagraSerialize(index->res, filename, index->idx, true) != CUVS_SUCCESS) {
    fprintf(stderr, "[seekdb_cuvs] serialize failed: %s\n", cuvsGetLastErrorText());
    return 1;
  }
  (void)cuvsStreamSync(index->res);
  return 0;
}

void* seekdb_cuvs_deserialize(const char* filename, long n, long dim)
{
  if (!filename || n <= 0 || dim <= 0) return NULL;
  sk_cuvs_index* index = (sk_cuvs_index*)calloc(1, sizeof(*index));
  if (!index) return NULL;
  if (cuvsResourcesCreate(&index->res) != CUVS_SUCCESS) goto fail;
  if (cuvsStreamGet(index->res, &index->stream) != CUVS_SUCCESS) { index->stream = 0; }
  if (cuvsCagraIndexParamsCreate(&index->ip) != CUVS_SUCCESS) goto fail_res;
  if (cuvsCagraSearchParamsCreate(&index->sp) != CUVS_SUCCESS) goto fail_ip;
  if (cuvsCagraIndexCreate(&index->idx) != CUVS_SUCCESS) goto fail_sp;
  if (cuvsCagraDeserialize(index->res, filename, index->idx) != CUVS_SUCCESS) goto fail_idx;
  {
    int64_t actual_n = 0;
    int64_t actual_dim = 0;
    if (cuvsCagraIndexGetSize(index->idx, &actual_n) != CUVS_SUCCESS ||
        cuvsCagraIndexGetDims(index->idx, &actual_dim) != CUVS_SUCCESS ||
        actual_n != n || actual_dim != dim) {
      fprintf(stderr, "[seekdb_cuvs] snapshot shape mismatch: %ld x %ld, expected %ld x %ld\n",
              (long)actual_n, (long)actual_dim, n, dim);
      goto fail_idx;
    }
    int gdeg = 64, ideg = 128, niter = 38;
    choose_build_params(n, dim, &gdeg, &ideg, &niter);
    index->graph_degree = gdeg;
    int64_t actual_deg = 0;
    if (cuvsCagraIndexGetGraphDegree(index->idx, &actual_deg) == CUVS_SUCCESS && actual_deg > 0) {
      index->graph_degree = (int)actual_deg;
    }
  }
  index->n = n;
  index->dim = dim;
  (void)ensure_search_scratch(index, 1, 100);
  return index;
fail_idx:
  cuvsCagraIndexDestroy(index->idx);
fail_sp:
  cuvsCagraSearchParamsDestroy(index->sp);
fail_ip:
  cuvsCagraIndexParamsDestroy(index->ip);
fail_res:
  cuvsResourcesDestroy(index->res);
fail:
  fprintf(stderr, "[seekdb_cuvs] deserialize failed: %s\n", cuvsGetLastErrorText());
  free(index);
  return NULL;
}

int seekdb_cuvs_cagra_knn(const float* base, long n, long dim,
                          const float* query, long nq, long topk,
                          unsigned int* out_ids)
{
  void* h = seekdb_cuvs_build(base, n, dim);
  if (!h) return 10;
  int rc = seekdb_cuvs_search(h, query, nq, topk, out_ids, NULL);
  seekdb_cuvs_free(h);
  return rc;
}
