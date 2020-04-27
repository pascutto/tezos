let src = Logs.Src.create "irmin.pack.commons" ~doc:"irmin-pack backend"

module Log = (val Logs.src_log src : Logs.LOG)

let current_version = "00000001"

module Default = struct
  let fresh = false

  let lru_size = 100_000

  let index_log_size = 500_000

  let readonly = false
end

let fresh_key =
  Irmin.Private.Conf.key ~doc:"Start with a fresh disk." "fresh"
    Irmin.Private.Conf.bool Default.fresh

let lru_size_key =
  Irmin.Private.Conf.key ~doc:"Size of the LRU cache for pack entries."
    "lru-size" Irmin.Private.Conf.int Default.lru_size

let index_log_size_key =
  Irmin.Private.Conf.key ~doc:"Size of index logs." "index-log-size"
    Irmin.Private.Conf.int Default.index_log_size

let readonly_key =
  Irmin.Private.Conf.key ~doc:"Start with a read-only disk." "readonly"
    Irmin.Private.Conf.bool Default.readonly

let fresh config = Irmin.Private.Conf.get config fresh_key

let lru_size config = Irmin.Private.Conf.get config lru_size_key

let readonly config = Irmin.Private.Conf.get config readonly_key

let index_log_size config = Irmin.Private.Conf.get config index_log_size_key

let root_key = Irmin.Private.Conf.root

let root config =
  match Irmin.Private.Conf.get config root_key with
  | None -> failwith "no root set"
  | Some r -> r

let config ?(fresh = Default.fresh) ?(readonly = Default.readonly)
    ?(lru_size = Default.lru_size) ?(index_log_size = Default.index_log_size)
    root =
  let config = Irmin.Private.Conf.empty in
  let config = Irmin.Private.Conf.add config fresh_key fresh in
  let config = Irmin.Private.Conf.add config root_key (Some root) in
  let config = Irmin.Private.Conf.add config lru_size_key lru_size in
  let config =
    Irmin.Private.Conf.add config index_log_size_key index_log_size
  in
  let config = Irmin.Private.Conf.add config readonly_key readonly in
  config

let ( ++ ) = Int64.add

let with_cache = IO.with_cache

open Lwt.Infix
module Pack = Pack
module Dict = Pack_dict
module Index = Pack_index
module IO = IO.Unix

exception RO_Not_Allowed = IO.RO_Not_Allowed

module Table (K : Irmin.Type.S) = Hashtbl.Make (struct
  type t = K.t

  let hash (t : t) = Irmin.Type.short_hash K.t t

  let equal (x : t) (y : t) = Irmin.Type.equal K.t x y
end)

module Atomic_write (K : Irmin.Type.S) (V : Irmin.Hash.S) = struct
  module Tbl = Table (K)
  module W = Irmin.Private.Watch.Make (K) (V)

  type key = K.t

  type value = V.t

  type watch = W.watch

  type t = {
    index : int64 Tbl.t;
    cache : V.t Tbl.t;
    block : IO.t;
    lock : Lwt_mutex.t;
    w : W.t;
    mutable open_instances : int;
  }

  let read_length32 ~off block =
    let buf = Bytes.create 4 in
    let n = IO.read block ~off buf in
    assert (n = 4);
    let n, v = Irmin.Type.(decode_bin int32) (Bytes.unsafe_to_string buf) 0 in
    assert (n = 4);
    Int32.to_int v

  let entry = Irmin.Type.(pair (string_of `Int32) V.t)

  let set_entry t ?off k v =
    let k = Irmin.Type.to_bin_string K.t k in
    let buf = Irmin.Type.to_bin_string entry (k, v) in
    match off with
    | None -> IO.append t.block buf
    | Some off -> IO.set t.block buf ~off

  let pp_branch = Irmin.Type.pp K.t

  let zero =
    match Irmin.Type.of_bin_string V.t (String.make V.hash_size '\000') with
    | Ok x -> x
    | Error _ -> assert false

  let refill t ~from =
    let len = IO.force_offset t.block in
    let rec aux offset =
      if offset >= len then ()
      else
        let len = read_length32 ~off:offset t.block in
        let buf = Bytes.create (len + V.hash_size) in
        let off = offset ++ 4L in
        let n = IO.read t.block ~off buf in
        assert (n = Bytes.length buf);
        let buf = Bytes.unsafe_to_string buf in
        let h =
          let h = String.sub buf 0 len in
          match Irmin.Type.of_bin_string K.t h with
          | Ok k -> k
          | Error (`Msg e) -> failwith e
        in
        let n, v = Irmin.Type.decode_bin V.t buf len in
        assert (n = String.length buf);
        if not (Irmin.Type.equal V.t v zero) then Tbl.add t.cache h v;
        Tbl.add t.index h offset;
        (aux [@tailcall]) (off ++ Int64.(of_int @@ (len + V.hash_size)))
    in
    aux from

  let sync_offset t =
    if IO.force_refill t.block then refill t ~from:0L
    else
      let former_log_offset = IO.offset t.block in
      let log_offset = IO.force_offset t.block in
      if log_offset > former_log_offset then refill t ~from:former_log_offset

  let unsafe_find t k =
    Log.debug (fun l -> l "[branches] find %a" pp_branch k);
    if IO.readonly t.block then sync_offset t;
    try Lwt.return_some (Tbl.find t.cache k) with Not_found -> Lwt.return_none

  let find t k = Lwt_mutex.with_lock t.lock (fun () -> unsafe_find t k)

  let unsafe_mem t k =
    Log.debug (fun l -> l "[branches] mem %a" pp_branch k);
    try Lwt.return (Tbl.mem t.cache k) with Not_found -> Lwt.return_false

  let mem t v = Lwt_mutex.with_lock t.lock (fun () -> unsafe_mem t v)

  let unsafe_remove t k =
    Tbl.remove t.cache k;
    try
      let off = Tbl.find t.index k in
      set_entry t ~off k zero
    with Not_found -> ()

  let remove t k =
    Log.debug (fun l -> l "[branches] remove %a" pp_branch k);
    Lwt_mutex.with_lock t.lock (fun () ->
        unsafe_remove t k;
        Lwt.return_unit)
    >>= fun () -> W.notify t.w k None

  let unsafe_clear t =
    IO.clear t.block;
    Tbl.clear t.cache;
    Tbl.clear t.index

  let clear t =
    Log.debug (fun l -> l "[branches] clear");
    Lwt_mutex.with_lock t.lock (fun () ->
        unsafe_clear t;
        Lwt.return_unit)

  let create = Lwt_mutex.create ()

  let watches = W.v ()

  let valid t =
    if t.open_instances <> 0 then (
      t.open_instances <- t.open_instances + 1;
      true )
    else false

  let unsafe_v ~fresh ~readonly file =
    let block = IO.v ~fresh ~version:current_version ~readonly file in
    let cache = Tbl.create 997 in
    let index = Tbl.create 997 in
    let t =
      {
        cache;
        index;
        block;
        w = watches;
        lock = Lwt_mutex.create ();
        open_instances = 1;
      }
    in
    refill t ~from:0L;
    t

  let (`Staged unsafe_v) =
    with_cache ~clear:unsafe_clear ~valid
      ~v:(fun () -> unsafe_v)
      "store.branches"

  let v ?fresh ?readonly file =
    Lwt_mutex.with_lock create (fun () ->
        let v = unsafe_v () ?fresh ?readonly file in
        Lwt.return v)

  let unsafe_set t k v =
    try
      let off = Tbl.find t.index k in
      Tbl.replace t.cache k v;
      set_entry t ~off k v
    with Not_found ->
      let offset = IO.offset t.block in
      set_entry t k v;
      Tbl.add t.cache k v;
      Tbl.add t.index k offset

  let set t k v =
    Log.debug (fun l -> l "[branches] set %a" pp_branch k);
    Lwt_mutex.with_lock t.lock (fun () ->
        unsafe_set t k v;
        Lwt.return_unit)
    >>= fun () -> W.notify t.w k (Some v)

  let unsafe_test_and_set t k ~test ~set =
    let v = try Some (Tbl.find t.cache k) with Not_found -> None in
    if not (Irmin.Type.(equal (option V.t)) v test) then Lwt.return_false
    else
      let return () = Lwt.return_true in
      match set with
      | None -> unsafe_remove t k |> return
      | Some v -> unsafe_set t k v |> return

  let test_and_set t k ~test ~set =
    Log.debug (fun l -> l "[branches] test-and-set %a" pp_branch k);
    Lwt_mutex.with_lock t.lock (fun () -> unsafe_test_and_set t k ~test ~set)
    >>= function
    | true -> W.notify t.w k set >|= fun () -> true
    | false -> Lwt.return_false

  let list t =
    Log.debug (fun l -> l "[branches] list");
    let keys = Tbl.fold (fun k _ acc -> k :: acc) t.cache [] in
    Lwt.return keys

  let watch_key t = W.watch_key t.w

  let watch t = W.watch t.w

  let unwatch t = W.unwatch t.w

  let unsafe_close t =
    t.open_instances <- t.open_instances - 1;
    if t.open_instances = 0 then (
      Tbl.reset t.index;
      Tbl.reset t.cache;
      if not (IO.readonly t.block) then IO.sync t.block;
      IO.close t.block;
      W.clear t.w )
    else Lwt.return_unit

  let close t = Lwt_mutex.with_lock t.lock (fun () -> unsafe_close t)

  let sync t = IO.sync t.block
end

module type CONFIG = Inode.CONFIG
