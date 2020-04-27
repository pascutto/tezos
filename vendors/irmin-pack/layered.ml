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
 * MERCHANTABILITY AND FITNESIrmin. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

let src = Logs.Src.create "irmin.layered" ~doc:"Irmin layered store"

module Log = (val Logs.src_log src : Logs.LOG)

module IO = Layers_IO.Unix
open Lwt.Infix

module type LAYERED_S = sig
  include Pack.S

  module U : Pack.S

  module L : Pack.S

  type 'a upper = 'a U.t

  val v :
    'a upper ->
    'a upper ->
    ?fresh:bool ->
    ?readonly:bool ->
    ?lru_size:int ->
    index:index ->
    string ->
    Lwt_mutex.t ->
    bool ->
    IO.t ->
    'a t Lwt.t

  val batch : unit -> 'a Lwt.t

  val unsafe_append : 'a t -> key -> value -> unit Lwt.t

  val project :
    'a t ->
    [ `Read | `Write ] upper ->
    [ `Read | `Write ] upper ->
    [ `Read | `Write ] t

  val layer_id : [ `Read ] t -> key -> int Lwt.t

  type 'a layer_type =
    | Upper : [ `Read | `Write ] U.t layer_type
    | Lower : [ `Read | `Write ] L.t layer_type

  val already_in_dst : 'l layer_type * 'l -> key -> bool Lwt.t

  val check_and_copy :
    'l layer_type * 'l ->
    [ `Read ] t ->
    aux:(value -> unit Lwt.t) ->
    string ->
    key ->
    unit Lwt.t

  val batch_lower : 'a t -> ([ `Read | `Write ] L.t -> 'b Lwt.t) -> 'b Lwt.t

  val current_upper : 'a t -> 'a U.t

  val previous_upper : 'a t -> 'a U.t

  val lower : 'a t -> [ `Read ] L.t

  val flip_upper : 'a t -> unit
end

module type CA = sig
  include Pack.S

  module Key : Irmin.Hash.TYPED with type t = key and type value = value
end

exception Copy_error of string

module Copy
    (Key : Irmin.Hash.S)
    (SRC : Pack.S with type key = Key.t)
    (DST : Pack.S with type key = SRC.key and type value = SRC.value) =
struct
  let add_to_dst name add dk (k, v) =
    add v >>= fun k' ->
    if not (Irmin.Type.equal dk k k') then
      Fmt.kstrf
        (fun x -> Lwt.fail (Copy_error x))
        "%s import error: expected %a, got %a" name
        Irmin.Type.(pp dk)
        k
        Irmin.Type.(pp dk)
        k'
    else Lwt.return_unit

  let already_in_dst ~dst k =
    DST.mem dst k >|= function
    | true ->
        Log.debug (fun l -> l "already in dst %a" (Irmin.Type.pp Key.t) k);
        true
    | false -> false

  let copy ~src ~dst ~aux str k =
    Log.debug (fun l -> l "copy %s %a" str (Irmin.Type.pp Key.t) k);
    SRC.find src k >>= function
    | None -> Lwt.return_unit
    | Some v -> aux v >>= fun () -> add_to_dst str (DST.add dst) Key.t (k, v)

  let check_and_copy ~src ~dst ~aux str k =
    already_in_dst ~dst k >>= function
    | true -> Lwt.return_false
    | false -> copy ~src ~dst ~aux str k >|= fun () -> true
end

module Stats = Irmin_layers.Stats

module Content_addressable
    (H : Irmin.Hash.S)
    (Index : Pack_index.S)
    (S : Pack.S with type index = Index.t and type key = H.t) :
  LAYERED_S
    with type key = S.key
     and type value = S.value
     and type index = S.index
     and module U = S
     and type L.key = S.key
     and type L.value = S.value = struct
  type index = S.index

  type key = S.key

  type value = S.value

  module U = S
  module L = S

  type 'a upper = 'a U.t

  type 'a t = {
    lower : [ `Read ] L.t;
    mutable flip : bool;
    uppers : 'a U.t * 'a U.t;
    lock : Lwt_mutex.t;
    readonly : bool;
    flip_file : IO.t;
  }

  let current_upper t = if t.flip then fst t.uppers else snd t.uppers

  let previous_upper t = if t.flip then snd t.uppers else fst t.uppers

  let lower t = t.lower

  let log_current_upper t = if t.flip then "upper1" else "upper0"

  let log_previous_upper t = if t.flip then "upper0" else "upper1"

  let batch_lower t f = L.batch t.lower f

  let v upper1 upper0 ?fresh ?(readonly = false) ?lru_size ~index root lock flip
      flip_file =
    L.v ?fresh ~readonly ?lru_size ~index root >|= fun lower ->
    { lower; flip; uppers = (upper1, upper0); lock; readonly; flip_file }

  let add t v =
    Lwt_mutex.with_lock t.lock (fun () ->
        Log.debug (fun l -> l "add in %s" (log_current_upper t));
        let upper = current_upper t in
        U.add upper v)

  let unsafe_add t k v =
    Lwt_mutex.with_lock t.lock (fun () ->
        Log.debug (fun l -> l "unsafe_add in %s" (log_current_upper t));
        let upper = current_upper t in
        U.unsafe_add upper k v)

  let unsafe_append t k v =
    Lwt_mutex.with_lock t.lock (fun () ->
        Log.debug (fun l -> l "unsafe_append in %s" (log_current_upper t));
        let upper = current_upper t in
        U.unsafe_append upper k v;
        Lwt.return_unit)

  let update_flip t = t.flip <- IO.read_flip_file t.flip_file

  let find t k =
    if t.readonly then update_flip t;
    let current = current_upper t in
    let previous = previous_upper t in
    Log.debug (fun l -> l "find in %s" (log_current_upper t));
    U.find current k >>= function
    | Some v -> Lwt.return_some v
    | None -> (
        Log.debug (fun l -> l "find in %s" (log_previous_upper t));
        U.find previous k >>= function
        | Some v -> Lwt.return_some v
        | None ->
            Log.debug (fun l -> l "find in lower");
            L.find t.lower k )

  let unsafe_find t k =
    if t.readonly then update_flip t;
    let current = current_upper t in
    let previous = previous_upper t in
    Log.debug (fun l -> l "unsafe_find in %s" (log_current_upper t));
    match U.unsafe_find current k with
    | Some v -> Some v
    | None -> (
        Log.debug (fun l -> l "unsafe_find in %s" (log_previous_upper t));
        match U.unsafe_find previous k with
        | Some v -> Some v
        | None ->
            Log.debug (fun l -> l "unsafe_find in lower");
            L.unsafe_find t.lower k )

  let mem t k =
    if t.readonly then update_flip t;
    let current = current_upper t in
    let previous = previous_upper t in
    U.mem current k >>= function
    | true -> Lwt.return_true
    | false -> (
        U.mem previous k >>= function
        | true -> Lwt.return_true
        | false -> L.mem t.lower k )

  let unsafe_mem t k =
    if t.readonly then update_flip t;
    let current = current_upper t in
    let previous = previous_upper t in
    U.unsafe_mem current k || U.unsafe_mem previous k || L.unsafe_mem t.lower k

  let project t upper1 upper0 = { t with uppers = (upper1, upper0) }

  let batch () = failwith "don't call this function"

  let sync t =
    U.sync (fst t.uppers);
    U.sync (snd t.uppers);
    L.sync t.lower

  let close t =
    U.close (fst t.uppers) >>= fun () ->
    U.close (snd t.uppers) >>= fun () -> L.close t.lower

  type integrity_error = U.integrity_error

  let integrity_check ~offset ~length k t =
    let current = current_upper t in
    let previous = previous_upper t in
    let lower = t.lower in
    match
      ( U.integrity_check ~offset ~length k current,
        U.integrity_check ~offset ~length k previous,
        L.integrity_check ~offset ~length k lower )
    with
    | Ok (), Ok (), Ok () -> Ok ()
    | Error `Wrong_hash, _, _
    | _, Error `Wrong_hash, _
    | _, _, Error `Wrong_hash ->
        Error `Wrong_hash
    | Error `Absent_value, _, _
    | _, Error `Absent_value, _
    | _, _, Error `Absent_value ->
        Error `Absent_value

  let layer_id t k =
    Log.debug (fun l -> l "layer_id of %a" (Irmin.Type.pp H.t) k);
    U.mem (fst t.uppers) k >>= function
    | true -> Lwt.return 1
    | false -> (
        U.mem (snd t.uppers) k >>= function
        | true -> Lwt.return 0
        | false -> (
            L.mem t.lower k >>= function
            | true -> Lwt.return 2
            | false -> raise Not_found ) )

  module CopyUpper = Copy (H) (U) (U)
  module CopyLower = Copy (H) (U) (L)

  type 'a layer_type =
    | Upper : [ `Read | `Write ] U.t layer_type
    | Lower : [ `Read | `Write ] L.t layer_type

  let stats str = function
    | true -> (
        match str with
        | "Contents" -> Stats.copy_contents ()
        | "Node" -> Stats.copy_nodes ()
        | "Commit" -> Stats.copy_commits ()
        | _ -> failwith "unexpected type in stats" )
    | false -> ()

  let already_in_dst : type l. l layer_type * l -> key -> bool Lwt.t =
   fun (ltype, dst) ->
    match ltype with
    | Lower -> CopyLower.already_in_dst ~dst
    | Upper -> CopyUpper.already_in_dst ~dst

  let check_and_copy_to_lower t ~dst ~aux str k =
    CopyLower.check_and_copy ~src:(previous_upper t) ~dst ~aux str k
    >|= stats str

  let check_and_copy_to_current t ~dst ~aux str (k : key) =
    CopyUpper.check_and_copy ~src:(previous_upper t) ~dst ~aux str k
    >|= stats str

  let check_and_copy :
      type l.
      l layer_type * l ->
      [ `Read ] t ->
      aux:(value -> unit Lwt.t) ->
      string ->
      key ->
      unit Lwt.t =
   fun (ltype, dst) ->
    match ltype with
    | Lower -> check_and_copy_to_lower ~dst
    | Upper -> check_and_copy_to_current ~dst

  let clear t =
    U.clear (fst t.uppers) >>= fun () ->
    U.clear (snd t.uppers) >>= fun () -> L.clear t.lower

  let flip_upper t =
    Log.debug (fun l -> l "flip_upper to %s" (log_previous_upper t));
    t.flip <- not t.flip
end

module type LAYERED_MAKER = sig
  type key

  type index

  module Make (V : Pack.ELT with type hash := key) :
    LAYERED_S
      with type key = key
       and type value = V.t
       and type index = index
       and type U.index = index
       and type U.key = key
       and type L.key = key
       and type U.value = V.t
       and type L.value = V.t
end

module Pack_Maker
    (H : Irmin.Hash.S)
    (Index : Pack_index.S)
    (S : Pack.MAKER with type key = H.t and type index = Index.t) :
  LAYERED_MAKER with type key = S.key and type index = S.index = struct
  type index = S.index

  type key = S.key

  module Make (V : Pack.ELT with type hash := key) = struct
    module Upper = S.Make (V)
    include Content_addressable (H) (Index) (Upper)
  end
end

module type AW = sig
  include Irmin.ATOMIC_WRITE_STORE

  val v : ?fresh:bool -> ?readonly:bool -> string -> t Lwt.t

  val sync : t -> unit
end

module Atomic_write (K : Irmin.Branch.S) (A : AW with type key = K.t) : sig
  include AW with type key = A.key and type value = A.value

  val v :
    A.t ->
    A.t ->
    ?fresh:bool ->
    ?readonly:bool ->
    string ->
    Lwt_mutex.t ->
    bool ->
    IO.t ->
    t Lwt.t

  val copy :
    t ->
    mem_commit_lower:(value -> bool Lwt.t) ->
    mem_commit_upper:(value -> bool Lwt.t) ->
    unit Lwt.t

  val flip_upper : t -> unit
end = struct
  type key = A.key

  type value = A.value

  module U = A
  module L = A

  type t = {
    lower : L.t;
    mutable flip : bool;
    uppers : U.t * U.t;
    lock : Lwt_mutex.t;
    readonly : bool;
    flip_file : IO.t;
  }

  let current_upper t = if t.flip then fst t.uppers else snd t.uppers

  let previous_upper t = if t.flip then snd t.uppers else fst t.uppers

  let log_current_upper t = if t.flip then "upper1" else "upper0"

  let log_previous_upper t = if t.flip then "upper0" else "upper1"

  let update_flip t = t.flip <- IO.read_flip_file t.flip_file

  let mem t k =
    if t.readonly then update_flip t;
    let current = current_upper t in
    let previous = previous_upper t in
    Log.debug (fun l -> l "[branches] mem in %s" (log_current_upper t));
    U.mem current k >>= function
    | true -> Lwt.return_true
    | false -> (
        Log.debug (fun l -> l "[branches] mem in%s" (log_previous_upper t));
        U.mem previous k >>= function
        | true -> Lwt.return_true
        | false ->
            Log.debug (fun l -> l "[branches] mem in lower");
            L.mem t.lower k )

  let find t k =
    if t.readonly then update_flip t;
    let current = current_upper t in
    let previous = previous_upper t in
    Log.debug (fun l -> l "[branches] find in %s" (log_current_upper t));
    U.find current k >>= function
    | Some v -> Lwt.return_some v
    | None -> (
        Log.debug (fun l -> l "[branches] find in %s" (log_previous_upper t));
        U.find previous k >>= function
        | Some v -> Lwt.return_some v
        | None ->
            Log.debug (fun l -> l "[branches] find in lower");
            L.find t.lower k )

  let unsafe_set t k v =
    Log.debug (fun l ->
        l "unsafe set %a in %s" (Irmin.Type.pp K.t) k (log_current_upper t));
    let upper = current_upper t in
    U.set upper k v

  let set t k v = Lwt_mutex.with_lock t.lock (fun () -> unsafe_set t k v)

  (* we have to copy back into upper the branch against we want to do
     test and set *)
  let unsafe_test_and_set t k ~test ~set =
    let current = current_upper t in
    let previous = previous_upper t in
    let find_in_lower () =
      L.find t.lower k >>= function
      | None -> U.test_and_set current k ~test:None ~set
      | Some v ->
          U.set current k v >>= fun () -> U.test_and_set current k ~test ~set
    in
    U.mem current k >>= function
    | true -> U.test_and_set current k ~test ~set
    | false -> (
        U.find previous k >>= function
        | None -> find_in_lower ()
        | Some v ->
            U.set current k v >>= fun () -> U.test_and_set current k ~test ~set
        )

  let test_and_set t k ~test ~set =
    Lwt_mutex.with_lock t.lock (fun () -> unsafe_test_and_set t k ~test ~set)

  let remove t k =
    U.remove (fst t.uppers) k >>= fun () ->
    U.remove (snd t.uppers) k >>= fun () -> L.remove t.lower k

  let list t =
    U.list (fst t.uppers) >>= fun upper1 ->
    U.list (snd t.uppers) >>= fun upper2 ->
    L.list t.lower >|= fun lower ->
    List.fold_left
      (fun acc b -> if List.mem b acc then acc else b :: acc)
      lower (upper1 @ upper2)

  type watch = U.watch

  let watch t = U.watch (current_upper t)

  let watch_key t = U.watch_key (current_upper t)

  let unwatch t = U.unwatch (current_upper t)

  let close t =
    U.close (fst t.uppers) >>= fun () ->
    U.close (snd t.uppers) >>= fun () -> L.close t.lower

  let v upper1 upper0 ?fresh ?(readonly = false) file lock flip flip_file =
    L.v ?fresh ~readonly file >|= fun lower ->
    { lower; flip; uppers = (upper1, upper0); lock; readonly; flip_file }

  (** Do not copy branches that point to commits not copied. *)
  let copy t ~mem_commit_lower ~mem_commit_upper =
    let previous = previous_upper t in
    let current = current_upper t in
    U.list previous >>= fun branches ->
    Lwt_list.iter_p
      (fun branch ->
        U.find previous branch >>= function
        | None -> Lwt.fail_with "branch not found in previous upper"
        | Some hash -> (
            (mem_commit_lower hash >>= function
             | true ->
                 Log.debug (fun l ->
                     l "[branches] copy to lower %a" (Irmin.Type.pp K.t) branch);
                 Stats.copy_branches ();
                 L.set t.lower branch hash
             | false -> Lwt.return_unit)
            >>= fun () ->
            mem_commit_upper hash >>= function
            | true ->
                Log.debug (fun l ->
                    l "[branches] copy to current %a" (Irmin.Type.pp K.t) branch);
                Stats.copy_branches ();
                U.set current branch hash
            | false -> Lwt.return_unit ))
      branches

  let flip_upper t =
    Log.debug (fun l -> l "[branches] flip to %s" (log_previous_upper t));
    t.flip <- not t.flip

  let clear t =
    U.clear (fst t.uppers) >>= fun () ->
    U.clear (snd t.uppers) >>= fun () -> L.clear t.lower

  let sync t =
    U.sync (fst t.uppers);
    U.sync (snd t.uppers);
    L.sync t.lower
end
