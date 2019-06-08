(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2019 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(* Copyright (c) 2019 Nomadic Labs. <nomadic@tezcore.com>                    *)
(*                                                                           *)
(* Permission is hereby granted, free of charge, to any person obtaining a   *)
(* copy of this software and associated documentation files (the "Software"),*)
(* to deal in the Software without restriction, including without limitation *)
(* the rights to use, copy, modify, merge, publish, distribute, sublicense,  *)
(* and/or sell copies of the Software, and to permit persons to whom the     *)
(* Software is furnished to do so, subject to the following conditions:      *)
(*                                                                           *)
(* The above copyright notice and this permission notice shall be included   *)
(* in all copies or substantial portions of the Software.                    *)
(*                                                                           *)
(* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR*)
(* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  *)
(* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL   *)
(* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER*)
(* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING   *)
(* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER       *)
(* DEALINGS IN THE SOFTWARE.                                                 *)
(*                                                                           *)
(*****************************************************************************)

type error += Wrong_snapshot_export of History_mode.t * History_mode.t
type error += Wrong_block_export of
    Block_hash.t * [ `Pruned | `Too_few_predecessors | `Cannot_be_found ]
type error += Inconsistent_imported_block of Block_hash.t * Block_hash.t
type error += Snapshot_import_failure of string
type error += Wrong_protocol_hash of Protocol_hash.t
type error += Inconsistent_operation_hashes of
    (Operation_list_list_hash.t * Operation_list_list_hash.t)
type error += Wrong_reconstruct_mode

val export:
  ?export_rolling:bool ->
  context_index:Context.index ->
  store:Store.t ->
  genesis:Block_hash.t ->
  string ->
  string option ->
  unit tzresult Lwt.t

val import:
  ?reconstruct:bool ->
  data_dir:string ->
  dir_cleaner:(string -> unit Lwt.t) ->
  patch_context:('a option -> Context.t -> Context.t Lwt.t) ->
  genesis:State.Chain.genesis ->
  string ->
  string option ->
  unit tzresult Lwt.t

val reconstruct_contexts_exposed:
  Chain_id.t ->
  block:string option ->
  Raw_store.t -> State.Chain.t -> Context.index -> unit tzresult Lwt.t
