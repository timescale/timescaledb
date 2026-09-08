/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <executor/instrument.h>

/*
 * Mirror the external statement-tracking provider's public rendezvous contract.
 * Its installed header is not a build dependency because the components build
 * and release independently. Keep the rendezvous names, types, layouts, and
 * size rules in sync with that contract.
 */
#define TSS_CALLBACKS_VAR_NAME "tss_callbacks"
#define TSS_CALLBACKS_VERSION INT64_C(1)

struct BufferUsage;
struct WalUsage;

typedef void (*tss_store_hook_type)(const char *query, int query_location, int query_len,
									uint64_t query_id, uint64_t total_time, uint64_t rows,
									const struct BufferUsage *bufusage,
									const struct WalUsage *walusage);
typedef bool (*tss_enabled_hook_type)(int level);

typedef struct TSSCallbacks
{
	int64_t version_num;
	tss_store_hook_type tss_store_hook;
	tss_enabled_hook_type tss_enabled;
} TSSCallbacks;

#define TSS_STATEMENT_SCOPE_CALLBACKS_VAR_NAME "tss_statement_scope_callbacks"
#define TSS_STATEMENT_SCOPE_CALLBACKS_VERSION INT64_C(1)
#define TSS_STATEMENT_SCOPE_NOT_ENTERED (-INT64_C(1))

typedef int64_t TSSStatementScopeToken;
typedef TSSStatementScopeToken (*tss_statement_scope_enter_hook_type)(void);
typedef void (*tss_statement_scope_exit_hook_type)(TSSStatementScopeToken scope_token);
typedef bool (*tss_statement_scope_reset_hook_type)(void);

typedef struct TSSStatementScopeCallbacks
{
	int64_t version_num;
	int64_t struct_size;
	tss_statement_scope_enter_hook_type enter;
	tss_statement_scope_exit_hook_type exit;
	tss_statement_scope_reset_hook_type reset;
} TSSStatementScopeCallbacks;

#define TSS_STATEMENT_SCOPE_CALLBACKS_V1_MIN_SIZE                                                  \
	((int64_t) (offsetof(TSSStatementScopeCallbacks, reset) +                                      \
				sizeof(((TSSStatementScopeCallbacks *) 0)->reset)))

static inline bool
tss_statement_scope_callbacks_usable(const TSSStatementScopeCallbacks *callbacks)
{
	return callbacks != NULL && callbacks->version_num == TSS_STATEMENT_SCOPE_CALLBACKS_VERSION &&
		   callbacks->struct_size >= TSS_STATEMENT_SCOPE_CALLBACKS_V1_MIN_SIZE &&
		   callbacks->enter != NULL && callbacks->exit != NULL && callbacks->reset != NULL;
}

typedef struct TsTssStatementScope
{
	tss_statement_scope_exit_hook_type exit;
	tss_store_hook_type store;
	TSSStatementScopeToken scope_token;
	bool synthetic_enabled;
	instr_time start_time;
	BufferUsage start_bufusage;
	WalUsage start_walusage;
} TsTssStatementScope;

#define TS_TSS_STATEMENT_SCOPE_INIT                                                                \
	{                                                                                              \
		.scope_token = TSS_STATEMENT_SCOPE_NOT_ENTERED                                             \
	}

extern void ts_begin_tss_store_callback(volatile TsTssStatementScope *scope);
extern bool ts_tss_store_callback_started(const volatile TsTssStatementScope *scope);
extern void ts_tss_statement_scope_enter(volatile TsTssStatementScope *scope);
extern void ts_tss_statement_scope_exit(volatile TsTssStatementScope *scope);
extern void ts_end_tss_store_callback(volatile TsTssStatementScope *scope, const char *query,
									  int query_location, int query_len, uint64 query_id,
									  uint64 rows);
