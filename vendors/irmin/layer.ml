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

open Lwt.Infix

let src = Logs.Src.create "irmin.layered" ~doc:"Irmin layered store"

let root conf = match Conf.get conf Conf.root with None -> "" | Some r -> r

module Log = (val Logs.src_log src : Logs.LOG)

let reset_lock = Lwt_mutex.create ()

let freeze_lock = Lwt_mutex.create ()

module type STORE = sig
  include S.STORE

  val freeze :
    ?min:commit list -> ?max:commit list -> ?squash:bool -> repo -> unit Lwt.t

  type store_handle =
    | Commit_t : hash -> store_handle
    | Node_t : hash -> store_handle
    | Content_t : hash -> store_handle

  val layer_id : repo -> store_handle -> string Lwt.t

  val async_freeze : repo -> bool

  val wait_for_freeze : unit -> unit Lwt.t
end

module type CONF = sig
  val lower_name : string
end

module type CA = sig
  include S.CONTENT_ADDRESSABLE_STORE

  module Key : S.TYPED_HASH with type t = key and type value = value
end

exception Copy_error of string

module Copy
    (SRC : S.CONTENT_ADDRESSABLE_STORE)
    (DST : CA with type key = SRC.key and type value = SRC.value) =
struct
  let add_to_dst name add dk (k, v) =
    add v >>= fun k' ->
    if not (Type.equal dk k k') then
      Fmt.kstrf
        (fun x -> Lwt.fail (Copy_error x))
        "%s import error: expected %a, got %a" name
        Type.(pp dk)
        k
        Type.(pp dk)
        k'
    else Lwt.return_unit

  let already_in_dst ~dst k add =
    DST.mem dst k >>= function
    | true ->
        Log.debug (fun l -> l "already in dst %a" (Type.pp DST.Key.t) k);
        Lwt.return_unit
    | false -> add k

  let copy ~src ~dst ~aux str k =
    Log.debug (fun l -> l "copy %s %a" str (Type.pp DST.Key.t) k);
    SRC.find src k >>= function
    | None -> Lwt.return_unit
    | Some v -> aux v >>= fun () -> add_to_dst str (DST.add dst) DST.Key.t (k, v)

  let check_and_copy ~src ~dst ~aux str k =
    already_in_dst ~dst k (copy ~src ~dst ~aux str)
end

module Content_addressable
    (K : S.HASH)
    (V : Type.S)
    (L : CA with type key = K.t and type value = V.t)
    (U : S.CONTENT_ADDRESSABLE_STORE with type key = K.t and type value = V.t) : sig
  include S.CONTENT_ADDRESSABLE_STORE with type key = K.t and type value = V.t

  val v : 'a U.t -> [ `Read ] L.t -> 'a t

  val project :
    [ `Read | `Write ] U.t ->
    [ `Read | `Write ] U.t option ->
    [ `Read ] t ->
    [ `Read | `Write ] t

  val layer_id : [ `Read ] t -> key -> int Lwt.t

  val clear_upper : 'a t -> unit Lwt.t

  val next_upper : 'a t -> 'a U.t -> unit

  val reset_upper : 'a t -> unit

  val already_in_dst :
    dst:[ `Read | `Write ] L.t -> key -> (key -> unit Lwt.t) -> unit Lwt.t

  val copy :
    [ `Read ] t ->
    dst:[ `Read | `Write ] L.t ->
    aux:(value -> unit Lwt.t) ->
    string ->
    key ->
    unit Lwt.t

  val check_and_copy :
    [ `Read ] t ->
    dst:[ `Read | `Write ] L.t ->
    aux:(value -> unit Lwt.t) ->
    string ->
    key ->
    unit Lwt.t
end = struct
  type key = K.t

  type value = V.t

  type 'a t = {
    mutable upper : 'a U.t;
    lower : [ `Read ] L.t;
    (* this store is used instead of upper, while an async freeze is ongoing *)
    mutable tmp : 'a U.t option;
  }

  module Copy = Copy (U) (L)

  let already_in_dst = Copy.already_in_dst

  let check_and_copy t ~dst ~aux str k =
    Copy.check_and_copy ~src:t.upper ~dst ~aux str k

  let copy t ~dst ~aux str k = Copy.copy ~src:t.upper ~dst ~aux str k

  let find t k =
    let find_in_upper () =
      Log.debug (fun l -> l "find in upper");
      U.find t.upper k >>= function
      | Some v -> Lwt.return_some v
      | None -> L.find t.lower k
    in
    match t.tmp with
    | Some tmp -> (
        Log.debug (fun l -> l "find in tmp");
        U.find tmp k >>= function
        | Some v -> Lwt.return_some v
        | None -> find_in_upper () )
    | None -> find_in_upper ()

  let mem t k =
    let mem_in_upper () =
      Log.debug (fun l -> l "mem in upper");
      U.mem t.upper k >>= function
      | true -> Lwt.return_true
      | false -> L.mem t.lower k
    in
    match t.tmp with
    | Some tmp -> (
        Log.debug (fun l -> l "mem in tmp");
        U.mem tmp k >>= function
        | true -> Lwt.return_true
        | false -> mem_in_upper () )
    | None -> mem_in_upper ()

  let add t v =
    match t.tmp with
    | Some tmp ->
        Log.debug (fun l -> l "add %a in tmp" (Type.pp V.t) v);
        Lwt_mutex.with_lock reset_lock (fun () -> U.add tmp v)
    | None ->
        Log.debug (fun l -> l "add %a in upper" (Type.pp V.t) v);
        U.add t.upper v

  let unsafe_add t k v =
    match t.tmp with
    | Some tmp ->
        Lwt_mutex.with_lock reset_lock (fun () -> U.unsafe_add tmp k v)
    | None -> U.unsafe_add t.upper k v

  let v upper lower = { upper; lower; tmp = None }

  let project upper tmp t = { t with upper; tmp }

  let layer_id t k =
    Log.debug (fun l -> l "layer_id of %a" (Type.pp K.t) k);
    let layer_id_upper () =
      U.mem t.upper k >>= function
      | true -> Lwt.return (-1)
      | false -> (
          L.mem t.lower k >>= function
          | true -> Lwt.return 0
          | false -> raise Not_found )
    in
    match t.tmp with
    | None -> layer_id_upper ()
    | Some tmp -> (
        U.mem tmp k >>= function
        | true -> Lwt.return (-1)
        | false -> layer_id_upper () )

  let clear_upper t = U.clear t.upper

  let clear t =
    (match t.tmp with None -> Lwt.return_unit | Some tmp -> U.clear tmp)
    >>= fun () ->
    U.clear t.upper >>= fun () -> L.clear t.lower

  let next_upper t next_upper =
    Log.debug (fun l -> l "next_upper");
    match t.tmp with
    | None -> t.tmp <- Some next_upper
    | Some _ -> failwith "next_upper cannot have a non empty tmp"

  let reset_upper t =
    Log.debug (fun l -> l "reset_upper");
    match t.tmp with
    | None -> failwith "reset_upper cannot have tmp empty"
    | Some upper ->
        t.upper <- upper;
        t.tmp <- None
end

module Atomic_write
    (K : Type.S)
    (V : S.HASH)
    (L : S.ATOMIC_WRITE_STORE with type key = K.t and type value = V.t)
    (U : S.ATOMIC_WRITE_STORE with type key = K.t and type value = V.t) : sig
  include S.ATOMIC_WRITE_STORE with type key = K.t and type value = V.t

  val v : U.t -> L.t -> t

  val clear_upper : t -> unit Lwt.t

  val copy : t -> (value -> bool Lwt.t) -> unit Lwt.t

  val next_upper : t -> U.t -> unit

  val reset_upper : t -> unit
end = struct
  type t = { mutable upper : U.t; lower : L.t; mutable tmp : U.t option }

  type key = K.t

  type value = V.t

  let mem t k =
    let mem_in_upper () =
      Log.debug (fun l -> l "[branches] mem in upper");
      U.mem t.upper k >>= function
      | true -> Lwt.return_true
      | false ->
          Log.debug (fun l -> l "[branches] mem in lower");
          L.mem t.lower k
    in
    match t.tmp with
    | Some tmp -> (
        Log.debug (fun l -> l "[branches] mem in tmp");
        U.mem tmp k >>= function
        | true -> Lwt.return_true
        | false -> mem_in_upper () )
    | None -> mem_in_upper ()

  let find t k =
    let find_in_upper () =
      Log.debug (fun l -> l "[branches] find in upper");
      U.find t.upper k >>= function
      | Some v -> Lwt.return_some v
      | None ->
          Log.debug (fun l -> l "[branches] find in lower");
          L.find t.lower k
    in
    match t.tmp with
    | Some tmp -> (
        Log.debug (fun l -> l "[branches] find in tmp");
        U.find tmp k >>= function
        | Some v -> Lwt.return_some v
        | None -> find_in_upper () )
    | None -> find_in_upper ()

  let unsafe_set t k v =
    match t.tmp with
    | Some tmp ->
        Log.debug (fun l ->
            l "unsafe set %a %a in tmp" (Type.pp K.t) k (Type.pp V.t) v);
        U.set tmp k v
    | None ->
        Log.debug (fun l ->
            l "unsafe set %a %a in upper" (Type.pp K.t) k (Type.pp V.t) v);
        U.set t.upper k v

  let set t k v = Lwt_mutex.with_lock reset_lock (fun () -> unsafe_set t k v)

  (* we have to copy back into upper (or tmp) the branch against we want to do
     test and set *)
  let unsafe_test_and_set t k ~test ~set =
    let aux t ~find ~copy f =
      U.mem t k >>= function
      | true -> U.test_and_set t k ~test ~set
      | false -> (
          find k >>= function
          | None -> f ()
          | Some v -> copy k v >>= fun () -> U.test_and_set t k ~test ~set )
    in
    let find_in_lower tmp () =
      L.find t.lower k >>= function
      | None -> U.test_and_set tmp k ~test:None ~set
      | Some v -> U.set tmp k v >>= fun () -> U.test_and_set tmp k ~test ~set
    in
    match t.tmp with
    | Some tmp ->
        aux tmp ~find:(U.find t.upper) ~copy:(U.set tmp) (find_in_lower tmp)
    | None ->
        aux t.upper ~find:(L.find t.lower) ~copy:(U.set t.upper) (fun () ->
            U.test_and_set t.upper k ~test:None ~set)

  let test_and_set t k ~test ~set =
    Lwt_mutex.with_lock reset_lock (fun () ->
        unsafe_test_and_set t k ~test ~set)

  let remove t k =
    (match t.tmp with Some tmp -> U.remove tmp k | None -> Lwt.return_unit)
    >>= fun () ->
    U.remove t.upper k >>= fun () -> L.remove t.lower k

  let list t =
    (match t.tmp with Some tmp -> U.list tmp | None -> Lwt.return_nil)
    >>= fun tmp ->
    U.list t.upper >>= fun upper ->
    L.list t.lower >|= fun lower ->
    List.fold_left
      (fun acc b -> if List.mem b acc then acc else b :: acc)
      lower (tmp @ upper)

  type watch = U.watch

  let watch t = U.watch t.upper

  let watch_key t = U.watch_key t.upper

  let unwatch t = U.unwatch t.upper

  let close t = U.close t.upper >>= fun () -> L.close t.lower

  let clear_upper t = U.clear t.upper

  let clear t =
    (match t.tmp with None -> Lwt.return_unit | Some tmp -> U.clear tmp)
    >>= fun () ->
    U.clear t.upper >>= fun () -> L.clear t.lower

  let v upper lower = { upper; lower; tmp = None }

  let copy t commit_exists =
    U.list t.upper >>= fun branches ->
    Lwt_list.iter_p
      (fun branch ->
        U.find t.upper branch >>= function
        | None -> failwith "branch not found in src"
        | Some hash -> (
            (* Do not copy branches that point to commits not copied. *)
            commit_exists hash
            >>= function
            | true -> L.set t.lower branch hash
            | false -> Lwt.return_unit ))
      branches

  let next_upper t next_upper =
    Log.debug (fun l -> l "[branches] next_upper");
    match t.tmp with
    | None -> t.tmp <- Some next_upper
    | Some _ -> failwith "[branches] next_upper cannot have a non empty tmp"

  let reset_upper t =
    Log.debug (fun l -> l "[branches] reset_upper");
    match t.tmp with
    | None -> failwith "[branches] reset_upper cannot have tmp empty"
    | Some upper ->
        t.upper <- upper;
        t.tmp <- None
end

module Make
    (C : CONF)
    (L : S.STORE)
    (U : S.STORE
           with type step = L.step
            and type key = L.key
            and type metadata = L.metadata
            and type contents = L.contents
            and type hash = L.hash
            and type branch = L.branch
            and type Private.Node.value = L.Private.Node.value
            and type Private.Commit.value = L.Private.Commit.value) :
  STORE
    with type step = U.step
     and type key = U.key
     and type metadata = U.metadata
     and type contents = U.contents
     and type hash = U.hash
     and type branch = U.branch = struct
  type store_handle =
    | Commit_t : U.Private.Hash.t -> store_handle
    | Node_t : U.Private.Hash.t -> store_handle
    | Content_t : U.Private.Hash.t -> store_handle

  module X = struct
    module Hash = U.Private.Hash

    module Contents = struct
      module L = L.Private.Contents
      module U = U.Private.Contents

      module CA = struct
        module Key = Hash
        module Val = L.Val
        include Content_addressable (Key) (Val) (L) (U)
      end

      include Contents.Store (CA)
    end

    module Node = struct
      module L = L.Private.Node
      module U = U.Private.Node
      module P = U.Path
      module M = U.Metadata

      module CA = struct
        module Key = Hash
        module Val = L.Val
        include Content_addressable (Key) (Val) (L) (U)
      end

      include Node.Store (Contents) (P) (M) (CA)
    end

    module Commit = struct
      module L = L.Private.Commit
      module U = U.Private.Commit

      module CA = struct
        module Key = Hash
        module Val = L.Val
        include Content_addressable (Key) (Val) (L) (U)
      end

      include Commit.Store (Node) (CA)
    end

    module Branch = struct
      module L = L.Private.Branch
      module U = U.Private.Branch
      module Key = U.Key
      module Val = Hash
      include Atomic_write (Key) (Val) (L) (U)
    end

    module Slice = Slice.Make (Contents) (Node) (Commit)
    module Sync = Sync.None (Hash) (U.Branch)

    module Repo = struct
      module Upper = U
      module UP = U.Private
      module LP = L.Private
      module U = UP.Repo
      module L = LP.Repo

      type t = {
        contents : [ `Read ] Contents.CA.t;
        nodes : [ `Read ] Node.CA.t;
        commits : [ `Read ] Commit.CA.t;
        branch : Branch.t;
        mutable upper : U.t;
        lower : L.t;
        conf : Conf.t;
        mutable tmp : U.t option;
        mutable flip : bool;
        mutable closed : bool;
      }

      let contents_t t : 'a Contents.t = t.contents

      let node_t t : 'a Node.t = (contents_t t, t.nodes)

      let commit_t t : 'a Commit.t = (node_t t, t.commits)

      let branch_t t = t.branch

      let batch t f =
        match t.tmp with
        | None ->
            U.batch t.upper @@ fun b n c ->
            let contents_t = Contents.CA.project b None t.contents in
            let node_t = Node.CA.project n None t.nodes in
            let commit_t = Commit.CA.project c None t.commits in
            let node_t = (contents_t, node_t) in
            let commit_t = (node_t, commit_t) in
            f contents_t node_t commit_t
        | Some next_upper ->
            U.batch t.upper @@ fun b n c ->
            U.batch next_upper @@ fun nb nn nc ->
            let contents_t = Contents.CA.project b (Some nb) t.contents in
            let node_t = Node.CA.project n (Some nn) t.nodes in
            let commit_t = Commit.CA.project c (Some nc) t.commits in
            let node_t = (contents_t, node_t) in
            let commit_t = (node_t, commit_t) in
            f contents_t node_t commit_t

      let unsafe_close t =
        t.closed <- true;
        U.close t.upper >>= fun () ->
        L.close t.lower >>= fun () ->
        match t.tmp with None -> Lwt.return_unit | Some tmp -> U.close tmp

      let close t = Lwt_mutex.with_lock freeze_lock (fun () -> unsafe_close t)

      let v conf =
        U.v conf >>= fun upper ->
        let conf_lower =
          Conf.add conf Conf.root (Some (root conf ^ C.lower_name))
        in
        L.v conf_lower >|= fun lower ->
        let contents =
          Contents.CA.v (U.contents_t upper) (L.contents_t lower)
        in
        let nodes = Node.CA.v (U.node_t upper) (L.node_t lower) in
        let commits = Commit.CA.v (U.commit_t upper) (L.commit_t lower) in
        let branch = Branch.v (U.branch_t upper) (L.branch_t lower) in
        {
          contents;
          nodes;
          commits;
          branch;
          upper;
          lower;
          tmp = None;
          conf;
          flip = true;
          closed = false;
        }

      let change_upper t =
        let name =
          if t.flip then root t.conf ^ string_of_int 1 else root t.conf
        in
        t.flip <- not t.flip;
        Log.debug (fun l -> l "change_upper to store %s" name);
        let conf = Conf.add t.conf Conf.root (Some name) in
        U.v conf >|= fun next ->
        Contents.CA.next_upper t.contents (U.contents_t next);
        Node.CA.next_upper t.nodes (U.node_t next);
        Commit.CA.next_upper t.commits (U.commit_t next);
        Branch.next_upper t.branch (U.branch_t next);
        t.tmp <- Some next

      let reset_upper t =
        Contents.CA.reset_upper t.contents;
        Node.CA.reset_upper t.nodes;
        Commit.CA.reset_upper t.commits;
        Branch.reset_upper t.branch;
        match t.tmp with
        | None -> failwith "reset_upper cannot have tmp empty"
        | Some upper ->
            t.upper <- upper;
            t.tmp <- None

      let layer_id t store_handler =
        ( match store_handler with
        | Commit_t k -> Commit.CA.layer_id t.commits k
        | Node_t k -> Node.CA.layer_id t.nodes k
        | Content_t k -> Contents.CA.layer_id t.contents k )
        >|= fun a ->
        Log.debug (fun l -> l "layer_id got %d" a);
        match a with
        | -1 -> root t.conf
        | 0 -> root t.conf ^ C.lower_name
        | _ -> failwith "unexpected layer id"

      let clear_upper t =
        Contents.CA.clear_upper t.contents >>= fun () ->
        Node.CA.clear_upper t.nodes >>= fun () ->
        Commit.CA.clear_upper t.commits >>= fun () ->
        Branch.clear_upper t.branch

      let copy_contents t contents k =
        Contents.CA.check_and_copy t.contents ~dst:contents
          ~aux:(fun _ -> Lwt.return_unit)
          "Contents" k

      (* [root] is the root of the tree pointed by a commit *)
      let copy_nodes t nodes contents root =
        Node.CA.already_in_dst ~dst:nodes root (fun root ->
            let aux v =
              Lwt_list.iter_p
                (function
                  | _, `Contents (k, _) -> copy_contents t contents k
                  | _ -> Lwt.return_unit)
                (UP.Node.Val.list v)
            in
            let node k = Node.CA.copy t.nodes ~dst:nodes ~aux "Node" k in
            let skip k = LP.Node.mem nodes k in
            Upper.Repo.iter t.upper ~min:[] ~max:[ root ] ~node ~skip)

      let copy_branches t dst = Branch.copy t.branch (LP.Commit.mem dst)

      let copy_commit t contents nodes commits k =
        let aux c = copy_nodes t nodes contents (UP.Commit.Val.node c) in
        Commit.CA.check_and_copy t.commits ~dst:commits ~aux "Commit" k
        >>= fun () -> copy_branches t commits

      let copy t ?(squash = false) ?(min = []) ?(max = []) () =
        Log.debug (fun f -> f "copy");
        (match max with [] -> Upper.Repo.heads t.upper | m -> Lwt.return m)
        >>= fun max ->
        Lwt.catch
          (fun () ->
            (* If squash then copy only the commits in [max]. Otherwise copy each
               commit individually. *)
            ( if squash then
              Lwt_list.iter_p
                (fun k ->
                  L.batch t.lower (fun contents nodes commits ->
                      copy_commit t contents nodes commits (Upper.Commit.hash k)))
                max
            else
              Upper.Repo.export ~full:false ~min ~max:(`Max max) t.upper
              >>= fun slice ->
              L.batch t.lower (fun contents nodes commits ->
                  UP.Slice.iter slice (function
                    | `Commit (k, _) -> copy_commit t contents nodes commits k
                    | _ -> Lwt.return_unit)) )
            >|= fun () -> Ok ())
          (function
            | Copy_error e -> Lwt.return_error (`Msg e)
            | e -> Fmt.kstrf Lwt.fail_invalid_arg "copy error: %a" Fmt.exn e)

      let clear t =
        Contents.CA.clear t.contents >>= fun () ->
        Node.CA.clear t.nodes >>= fun () ->
        Commit.CA.clear t.commits >>= fun () ->
        Branch.clear t.branch >>= fun () ->
        let name =
          if t.flip then root t.conf ^ string_of_int 1 else root t.conf
        in
        let conf = Conf.add t.conf Conf.root (Some name) in
        U.v conf >>= fun next -> U.clear next

      let pre_copy t = change_upper t

      let post_copy t =
        clear_upper t >>= fun () ->
        Lwt_mutex.with_lock reset_lock (fun () ->
            reset_upper t;
            Lwt.return_unit)
        >|= fun () ->
        Lwt_mutex.unlock freeze_lock;
        Log.debug (fun l -> l "free lock")

      let async_freeze t = match t.tmp with None -> false | Some _ -> true
    end
  end

  include Store.Make (X)

  let conv_commit upper c =
    let hash = Commit.hash c in
    U.Commit.of_hash upper hash

  let unsafe_freeze ~min ~max ~squash t =
    Log.debug (fun l -> l "unsafe_freeze");
    X.Repo.pre_copy t >|= fun () ->
    Lwt.async (fun () ->
        Lwt.pause () >>= fun () ->
        X.Repo.copy t ~squash ~min ~max () >>= function
        | Ok () -> X.Repo.post_copy t
        | Error (`Msg e) ->
            Fmt.kstrf Lwt.fail_with "[gc_store]: import error %s" e);
    Log.debug (fun l -> l "after async called to copy")

  let freeze ?(min : commit list = []) ?(max : commit list = [])
      ?(squash = false) t =
    let upper = t.X.Repo.upper in
    let conv_commits =
      Lwt_list.fold_left_s
        (fun acc m ->
          conv_commit upper m >|= function None -> acc | Some c -> c :: acc)
        []
    in
    conv_commits min >>= fun min ->
    conv_commits max >>= fun max ->
    (* main thread takes the lock at the begining of freeze and async thread
       releases it at the end. This is to ensure that no two freezes can run
       simultaneously. *)
    Lwt_mutex.lock freeze_lock >>= fun () ->
    if t.closed then failwith "store is closed";
    unsafe_freeze ~min ~max ~squash t

  let layer_id = X.Repo.layer_id

  let async_freeze = X.Repo.async_freeze

  let wait_for_freeze () =
    Lwt_mutex.with_lock freeze_lock (fun () -> Lwt.return_unit)
end
