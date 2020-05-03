(*
 * Copyright (c) 2013-2020 Thomas Gazagnaire <thomas@gazagnaire.org>
 *                         Ioana Cristescu <ioana@tarides.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

module type S = sig
  include Irmin.S

  val freeze :
    ?min:commit list ->
    ?max:commit list ->
    ?squash:bool ->
    ?keep_max:bool ->
    ?heads:commit list ->
    ?recovery:bool ->
    repo ->
    unit Lwt.t

  type store_handle =
    | Commit_t : hash -> store_handle
    | Node_t : hash -> store_handle
    | Content_t : hash -> store_handle

  val layer_id : repo -> store_handle -> string Lwt.t
  (** [layer_id t store_handle] returns the layer where an object, identified by
      its hash, is stored. *)

  val async_freeze : unit -> bool
  (** [async_freeze t] returns true if there is an ongoing freeze. To be used
      with caution, as a freeze can start (or stop) just after the test. It is
      helpful when a single freeze is called, to check whether it completed or
      not. *)

  val upper_in_use : repo -> string
  (** [upper_in_use t] returns the name of the upper currently used. *)

  (** These modules should not be used. They are exposed purely for testing
      purposes. *)
  module PrivateLayer : sig
    module Hook : sig
      type 'a t

      val v : ('a -> unit Lwt.t) -> 'a t
    end

    val wait_for_freeze : unit -> unit Lwt.t

    val freeze_with_hook :
      ?min:commit list ->
      ?max:commit list ->
      ?squash:bool ->
      ?keep_max:bool ->
      ?heads:commit list ->
      ?recovery:bool ->
      ?hook:[ `After_Clear | `Before_Clear | `Before_Copy ] Hook.t ->
      repo ->
      unit Lwt.t
  end
end

module Make_ext
    (CA : Irmin.CONTENT_ADDRESSABLE_STORE_MAKER)
    (AW : Irmin.ATOMIC_WRITE_STORE_MAKER)
    (Metadata : Irmin.Metadata.S)
    (Contents : Irmin.Contents.S)
    (Path : Irmin.Path.S)
    (Branch : Irmin.Branch.S)
    (Hash : Irmin.Hash.S)
    (Node : Irmin.Private.Node.S
              with type metadata = Metadata.t
               and type hash = Hash.t
               and type step = Path.step)
    (Commit : Irmin.Private.Commit.S with type hash = Hash.t) :
  S
    with type key = Path.t
     and type contents = Contents.t
     and type branch = Branch.t
     and type hash = Hash.t
     and type step = Path.step
     and type metadata = Metadata.t
     and type Key.step = Path.step

module type S_MAKER = functor
  (M : Irmin.Metadata.S)
  (C : Irmin.Contents.S)
  (P : Irmin.Path.S)
  (B : Irmin.Branch.S)
  (H : Irmin.Hash.S)
  ->
  S
    with type key = P.t
     and type step = P.step
     and type metadata = M.t
     and type contents = C.t
     and type branch = B.t
     and type hash = H.t

module Make
    (CA : Irmin.CONTENT_ADDRESSABLE_STORE_MAKER)
    (AW : Irmin.ATOMIC_WRITE_STORE_MAKER) : S_MAKER

module Stats = Stats
