(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(* Copyright (c) 2018 Nomadic Labs. <nomadic@tezcore.com>                    *)
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


let (//) = Filename.concat
let context_dir data_dir = data_dir // "context"
let store_dir data_dir = data_dir // "store"

(** Main *)

module Term = struct

  let process args block =
    let run =
      Internal_event_unix.init () >>= fun () ->
      Node_shared_arg.read_data_dir args >>=? fun data_dir ->
      Node_data_version.ensure_data_dir data_dir >>=? fun () ->
      let context_root = context_dir data_dir in
      let store_root = store_dir data_dir in
      Store.init ~mapsize:40_960_000_000L store_root >>=? fun store ->
      let genesis = Genesis_chain.genesis in
      State.init
        ~context_root ~store_root genesis
        ~patch_context:(Patch_context.patch_context None) >>=? fun (state, chain_state, context_index, _history_mode') ->
      let chain_id = Chain_id.of_block_hash genesis.State.Chain.block in
      Store.Configuration.History_mode.read store >>=? fun history_mode ->
      begin
        if history_mode = History_mode.Full then
          Snapshots.reconstruct_contexts_exposed ~data_dir
            chain_id ~block store chain_state context_index
        else
          failwith "Full mode allowed only"
      end >>=? fun () ->
      Store.close store ;
      State.close state >>= fun () -> return_unit
    in
    match Lwt_main.run run with
    | Ok () -> `Ok ()
    | Error err ->
        `Error (false, Format.asprintf "%a" pp_print_error err)

  let block =
    let open Cmdliner.Arg in
    let doc ="Block hash from where you want to reconstruct" in
    value & opt (some string) None & info ~docv:"<block_hash>" ~doc ["block"]

  let term =
    let open Cmdliner.Term in
    ret (const process
         $ Node_shared_arg.Term.args
         $ block)

end

module Manpage = struct

  let command_description =
    "Reconstruct command"

  let description = [
    `S "DESCRIPTION" ;
  ]

  let options = [
    `S "OPTIONS" ;
  ]

  let examples =
    [
      `S "EXAMPLES" ;
    ]

  let man =
    description @
    options @
    examples @
    Node_shared_arg.Manpage.bugs

  let info =
    Cmdliner.Term.info
      ~doc:"Manage reconstruct"
      ~man
      "reconstruct"

end

let cmd =
  Term.term, Manpage.info
