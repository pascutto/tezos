open Common
open Lwt.Infix

let src = Logs.Src.create "irmin.pack.layers" ~doc:"irmin-pack backend"

module Log = (val Logs.src_log src : Logs.LOG)

module type CONFIG = Inode.CONFIG

module Default = struct
  let lower_root = "lower"

  let upper_root1 = "upper1"

  let upper_root0 = "upper0"

  let keep_max = false
end

module Conf = Irmin.Private.Conf

let lower_root_key =
  Conf.key ~doc:"The root directory for the lower layer." "root_lower"
    Conf.string Default.lower_root

let lower_root conf = Conf.get conf lower_root_key

let upper_root1_key =
  Conf.key ~doc:"The root directory for the upper layer." "root_upper"
    Conf.string Default.upper_root1

let upper_root1 conf = Conf.get conf upper_root1_key

let upper_root0_key =
  Conf.key ~doc:"The root directory for the secondary upper layer."
    "root_second" Conf.string Default.upper_root0

let upper_root0 conf = Conf.get conf upper_root0_key

let keep_max_key =
  Conf.key ~doc:"Keep the max commits in upper after a freeze." "keep-max"
    Conf.bool false

let get_keep_max conf = Conf.get conf keep_max_key

let config_layers ?(conf = Conf.empty) ?(lower_root = Default.lower_root)
    ?(upper_root1 = Default.upper_root1) ?(upper_root0 = Default.upper_root0)
    ?(keep_max = Default.keep_max) () =
  let config = Conf.add conf lower_root_key lower_root in
  let config = Conf.add config upper_root0_key upper_root0 in
  let config = Conf.add config upper_root1_key upper_root1 in
  let config = Conf.add config keep_max_key keep_max in
  config

module IO = Layers_IO.Unix
module Stats = Irmin_layers.Stats

let reset_lock = Lwt_mutex.create ()

let freeze_lock = Lwt_mutex.create ()

let may f = function None -> Lwt.return_unit | Some bf -> f bf

module Make_ext
    (Config : CONFIG)
    (M : Irmin.Metadata.S)
    (C : Irmin.Contents.S)
    (P : Irmin.Path.S)
    (B : Irmin.Branch.S)
    (H : Irmin.Hash.S)
    (Node : Irmin.Private.Node.S
              with type metadata = M.t
               and type hash = H.t
               and type step = P.step)
    (Commit : Irmin.Private.Commit.S with type hash = H.t) =
struct
  module Index = Pack_index.Make (H)
  module Pack = Pack.File (Index) (H)

  type store_handle =
    | Commit_t : H.t -> store_handle
    | Node_t : H.t -> store_handle
    | Content_t : H.t -> store_handle

  module X = struct
    module Hash = H

    type 'a value = { magic : char; hash : H.t; v : 'a }

    let value a =
      let open Irmin.Type in
      record "value" (fun hash magic v -> { magic; hash; v })
      |+ field "hash" H.t (fun v -> v.hash)
      |+ field "magic" char (fun v -> v.magic)
      |+ field "v" a (fun v -> v.v)
      |> sealr

    module Contents = struct
      module CA = struct
        module Key = H
        module Val = C

        module CA_Pack = Pack.Make (struct
          include Val
          module H = Irmin.Hash.Typed (H) (Val)

          let hash = H.hash

          let magic = 'B'

          let value = value Val.t

          let encode_bin ~dict:_ ~offset:_ v hash =
            Irmin.Type.encode_bin value { magic; hash; v }

          let decode_bin ~dict:_ ~hash:_ s off =
            let _, t = Irmin.Type.decode_bin ~headers:false value s off in
            t.v

          let magic _ = magic
        end)

        module CA = Closeable.Pack (CA_Pack)
        include Layered.Content_addressable (H) (Index) (CA)
      end

      include Irmin.Contents.Store (CA)
    end

    module Node = struct
      module Pa = Layered.Pack_Maker (H) (Index) (Pack)
      module CA = Inode.Make_layered (Config) (H) (Pa) (Node)
      include Irmin.Private.Node.Store (Contents) (P) (M) (CA)
    end

    module Commit = struct
      module CA = struct
        module Key = H
        module Val = Commit

        module CA_Pack = Pack.Make (struct
          include Val
          module H = Irmin.Hash.Typed (H) (Val)

          let hash = H.hash

          let value = value Val.t

          let magic = 'C'

          let encode_bin ~dict:_ ~offset:_ v hash =
            Irmin.Type.encode_bin value { magic; hash; v }

          let decode_bin ~dict:_ ~hash:_ s off =
            let _, v = Irmin.Type.decode_bin ~headers:false value s off in
            v.v

          let magic _ = magic
        end)

        module CA = Closeable.Pack (CA_Pack)
        include Layered.Content_addressable (H) (Index) (CA)
      end

      include Irmin.Private.Commit.Store (Node) (CA)
    end

    module Branch = struct
      module Key = B
      module Val = H
      module AW = Atomic_write (Key) (Val)
      module U = Closeable.Atomic_write (AW)
      include Layered.Atomic_write (Key) (U)
    end

    module Slice = Irmin.Private.Slice.Make (Contents) (Node) (Commit)
    module Sync = Irmin.Private.Sync.None (H) (B)

    module Repo = struct
      type upper_layer = {
        contents : [ `Read ] Contents.CA.U.t;
        node : [ `Read ] Node.CA.U.t;
        commit : [ `Read ] Commit.CA.U.t;
        branch : Branch.U.t;
        index : Index.t;
      }

      type t = {
        config : Irmin.Private.Conf.t;
        contents : [ `Read ] Contents.CA.t;
        node : [ `Read ] Node.CA.t;
        branch : Branch.t;
        commit : [ `Read ] Commit.CA.t;
        index : Index.t;
        uppers : upper_layer * upper_layer;
        mutable flip : bool;
        mutable closed : bool;
        flip_file : IO.t;
      }

      let contents_t t = t.contents

      let node_t t = (contents_t t, t.node)

      let commit_t t = (node_t t, t.commit)

      let branch_t t = t.branch

      let previous_upper t = if t.flip then snd t.uppers else fst t.uppers

      let current_upper t = if t.flip then fst t.uppers else snd t.uppers

      let log_current_upper t = if t.flip then "upper1" else "upper0"

      let log_previous_upper t = if t.flip then "upper0" else "upper1"

      let batch_upper (t : upper_layer) f =
        Commit.CA.U.batch t.commit (fun commit ->
            Node.CA.U.batch t.node (fun node ->
                Contents.CA.U.batch t.contents (fun contents ->
                    f contents node commit)))

      let batch t f =
        batch_upper (fst t.uppers) (fun b1 n1 c1 ->
            batch_upper (snd t.uppers) (fun b0 n0 c0 ->
                let contents = Contents.CA.project t.contents b1 b0 in
                let node = (contents, Node.CA.project t.node n1 n0) in
                let commit = (node, Commit.CA.project t.commit c1 c0) in
                f contents node commit))

      let v_upper root config =
        let fresh = fresh config in
        let lru_size = lru_size config in
        let readonly = readonly config in
        let log_size = index_log_size config in
        let index = Index.v ~fresh ~readonly ~log_size root in
        Contents.CA.U.v ~fresh ~readonly ~lru_size ~index root
        >>= fun contents ->
        Node.CA.U.v ~fresh ~readonly ~lru_size ~index root >>= fun node ->
        Commit.CA.U.v ~fresh ~readonly ~lru_size ~index root >>= fun commit ->
        Branch.U.v ~fresh ~readonly root >|= fun branch ->
        { index; contents; node; commit; branch }

      let v config =
        let root = root config in
        let upper1 = Filename.concat root (upper_root1 config) in
        v_upper upper1 config >>= fun upper1 ->
        let upper0 = Filename.concat root (upper_root0 config) in
        v_upper upper0 config >>= fun upper0 ->
        let flip_file =
          let file = Filename.concat root "flip" in
          let init = Bytes.make 1 (char_of_int 1) in
          IO.v file init
        in
        let flip = IO.read_flip_file flip_file in
        let root = Filename.concat root (lower_root config) in
        let fresh = fresh config in
        let lru_size = lru_size config in
        let readonly = readonly config in
        let log_size = index_log_size config in
        let index = Index.v ~fresh ~readonly ~log_size root in
        Contents.CA.v upper1.contents upper0.contents ~fresh ~readonly ~lru_size
          ~index root reset_lock flip flip_file
        >>= fun contents ->
        Node.CA.v upper1.node upper0.node ~fresh ~readonly ~lru_size ~index root
          reset_lock flip flip_file
        >>= fun node ->
        Commit.CA.v upper1.commit upper0.commit ~fresh ~readonly ~lru_size
          ~index root reset_lock flip flip_file
        >>= fun commit ->
        Branch.v upper1.branch upper0.branch ~fresh ~readonly root reset_lock
          flip flip_file
        >|= fun branch ->
        {
          contents;
          node;
          commit;
          branch;
          config;
          index;
          uppers = (upper1, upper0);
          flip;
          closed = false;
          flip_file;
        }

      let unsafe_close t =
        t.closed <- true;
        Index.close t.index;
        Index.close (fst t.uppers).index;
        Index.close (snd t.uppers).index;
        Contents.CA.close (contents_t t) >>= fun () ->
        Node.CA.close (snd (node_t t)) >>= fun () ->
        Commit.CA.close (snd (commit_t t)) >>= fun () -> Branch.close t.branch

      let close t = Lwt_mutex.with_lock freeze_lock (fun () -> unsafe_close t)

      let layer_id t store_handler =
        ( match store_handler with
        | Commit_t k -> Commit.CA.layer_id t.commit k
        | Node_t k -> Node.CA.layer_id t.node k
        | Content_t k -> Contents.CA.layer_id t.contents k )
        >|= function
        | 0 -> upper_root0 t.config
        | 1 -> upper_root1 t.config
        | 2 -> lower_root t.config
        | _ -> failwith "unexpected layer id"

      let clear_previous_upper t =
        Log.debug (fun l -> l "clear upper %s" (log_previous_upper t));
        IO.write_flip_file t.flip t.flip_file;
        let t = previous_upper t in
        Contents.CA.U.clear t.contents >>= fun () ->
        Node.CA.U.clear t.node >>= fun () ->
        Commit.CA.U.clear t.commit >>= fun () -> Branch.U.clear t.branch

      let flip_upper t =
        t.flip <- not t.flip;
        Contents.CA.flip_upper t.contents;
        Node.CA.flip_upper t.node;
        Commit.CA.flip_upper t.commit;
        Branch.flip_upper t.branch;
        Lwt.return_unit

      let upper_in_use t =
        if t.flip then upper_root1 t.config else upper_root0 t.config

      let sync t =
        Contents.CA.sync (contents_t t);
        Commit.CA.sync (snd (commit_t t));
        Branch.sync (branch_t t)
    end
  end

  let null =
    match Sys.os_type with
    | "Unix" | "Cygwin" -> "/dev/null"
    | "Win32" -> "NUL"
    | _ -> invalid_arg "invalid os type"

  let integrity_check ?ppf ~auto_repair t =
    let ppf =
      match ppf with
      | Some p -> p
      | None -> open_out null |> Format.formatter_of_out_channel
    in
    Fmt.pf ppf "Running the integrity_check.\n%!";
    let nb_commits = ref 0 in
    let nb_nodes = ref 0 in
    let nb_contents = ref 0 in
    let nb_absent = ref 0 in
    let nb_corrupted = ref 0 in
    let exception Cannot_fix in
    let contents = X.Repo.contents_t t in
    let nodes = X.Repo.node_t t |> snd in
    let commits = X.Repo.commit_t t |> snd in
    let pp_stats () =
      Fmt.pf ppf "\t%dk contents / %dk nodes / %dk commits\n%!"
        (!nb_contents / 1000) (!nb_nodes / 1000) (!nb_commits / 1000)
    in
    let count_increment count =
      incr count;
      if !count mod 1000 = 0 then pp_stats ()
    in
    let f (k, (offset, length, m)) =
      match m with
      | 'B' ->
          count_increment nb_contents;
          X.Contents.CA.integrity_check ~offset ~length k contents
      | 'N' | 'I' ->
          count_increment nb_nodes;
          X.Node.CA.integrity_check ~offset ~length k nodes
      | 'C' ->
          count_increment nb_commits;
          X.Commit.CA.integrity_check ~offset ~length k commits
      | _ -> invalid_arg "unknown content type"
    in
    if auto_repair then
      try
        Index.filter t.index (fun binding ->
            match f binding with
            | Ok () -> true
            | Error `Wrong_hash -> raise Cannot_fix
            | Error `Absent_value ->
                incr nb_absent;
                false);
        if !nb_absent = 0 then Ok `No_error else Ok (`Fixed !nb_absent)
      with Cannot_fix -> Error (`Cannot_fix "Not implemented")
    else (
      Index.iter
        (fun k v ->
          match f (k, v) with
          | Ok () -> ()
          | Error `Wrong_hash -> incr nb_corrupted
          | Error `Absent_value -> incr nb_absent)
        t.index;
      if !nb_absent = 0 && !nb_corrupted = 0 then Ok `No_error
      else Error (`Corrupted (!nb_corrupted + !nb_absent)) )

  include Irmin.Of_private (X)

  module Copy = struct
    let copy_branches t =
      let mem_commit_upper =
        X.Commit.CA.U.mem (X.Repo.current_upper t).commit
      in
      X.Commit.CA.batch_lower t.X.Repo.commit (fun commits ->
          let mem_commit_lower =
            X.Commit.CA.already_in_dst (X.Commit.CA.Lower, commits)
          in
          X.Branch.copy t.X.Repo.branch ~mem_commit_lower ~mem_commit_upper)

    let copy_contents contents t k =
      X.Contents.CA.check_and_copy contents t.X.Repo.contents
        ~aux:(fun _ -> Lwt.return_unit)
        "Contents" k

    let copy_tree ?(skip = fun _ -> Lwt.return_false) nodes contents t root =
      X.Node.CA.already_in_dst nodes root >>= function
      | true -> Lwt.return_unit
      | false ->
          let aux v =
            Lwt_list.iter_p
              (function
                | _, `Contents (k, _) -> copy_contents contents t k
                | _ -> Lwt.return_unit)
              (X.Node.Val.list v)
          in
          let node k =
            X.Node.CA.check_and_copy nodes t.X.Repo.node ~aux "Node" k
          in
          Repo.iter t ~min:[] ~max:[ root ] ~node ~skip

    let copy_commit ~copy_tree commits nodes contents t k =
      let aux c = copy_tree nodes contents t (X.Commit.Val.node c) in
      X.Commit.CA.check_and_copy commits t.X.Repo.commit ~aux "Commit" k

    module CopyToLower = struct
      let copy_tree nodes contents t root =
        let skip k = X.Node.CA.already_in_dst nodes k in
        copy_tree ~skip nodes contents t root

      let copy_commit commits nodes contents t k =
        copy_commit ~copy_tree commits nodes contents t k

      let batch_lower t f =
        X.Commit.CA.batch_lower t.X.Repo.commit (fun commits ->
            X.Node.CA.batch_lower t.X.Repo.node (fun nodes ->
                X.Contents.CA.batch_lower t.X.Repo.contents (fun contents ->
                    f commits nodes contents)))

      let copy_max_commits t (max : commit list) =
        Log.debug (fun f -> f "copy max commits to lower");
        Lwt_list.iter_p
          (fun k ->
            let h = Commit.hash k in
            batch_lower t (fun commits nodes contents ->
                copy_commit
                  (X.Commit.CA.Lower, commits)
                  (X.Node.CA.Lower, nodes)
                  (X.Contents.CA.Lower, contents)
                  t h))
          max

      let copy_slice t slice =
        batch_lower t (fun commits nodes contents ->
            X.Slice.iter slice (function
              | `Commit (k, _) ->
                  copy_commit
                    (X.Commit.CA.Lower, commits)
                    (X.Node.CA.Lower, nodes)
                    (X.Contents.CA.Lower, contents)
                    t k
              | _ -> Lwt.return_unit))

      let copy t ~min ~max () =
        Log.debug (fun f -> f "copy to lower");
        Repo.export ~full:false ~min ~max:(`Max max) t >>= copy_slice t
    end

    module CopyToUpper = struct
      let batch_current t f =
        X.Repo.batch t (fun contents nodes commits ->
            let contents = X.Contents.CA.current_upper contents in
            let nodes = X.Node.CA.current_upper (snd nodes) in
            let commits = X.Commit.CA.current_upper (snd commits) in
            f contents nodes commits)

      let copy_max_commits t max =
        Log.debug (fun f ->
            f "copy max commits to current %s" (X.Repo.log_current_upper t));
        Lwt_list.iter_p
          (fun k ->
            let h = Commit.hash k in
            batch_current t (fun contents nodes commits ->
                copy_commit ~copy_tree
                  (X.Commit.CA.Upper, commits)
                  (X.Node.CA.Upper, nodes)
                  (X.Contents.CA.Upper, contents)
                  t h))
          max

      let copy_slice t slice =
        X.Slice.iter slice (function
          | `Commit (k, _) ->
              batch_current t (fun contents nodes commits ->
                  copy_commit ~copy_tree
                    (X.Commit.CA.Upper, commits)
                    (X.Node.CA.Upper, nodes)
                    (X.Contents.CA.Upper, contents)
                    t k)
          | _ -> Lwt.return_unit)

      let empty_intersection slice commits =
        let ok = ref true in
        let includes k =
          List.exists (fun k' -> Irmin.Type.equal Hash.t k k') commits
        in
        X.Slice.iter slice (function
          | `Commit (k, _) ->
              if includes k then ok := false;
              Lwt.return_unit
          | _ -> Lwt.return_unit)
        >|= fun () -> !ok

      (** Copy the heads that include a max commit *)
      let copy_heads t max heads =
        Log.debug (fun f ->
            f "copy heads to current %s" (X.Repo.log_current_upper t));
        Lwt_list.iter_p
          (fun head ->
            Repo.export ~full:false ~min:max ~max:(`Max [ head ]) t
            >>= fun slice ->
            List.map (fun c -> Commit.hash c) max |> empty_intersection slice
            >>= function
            | false -> copy_slice t slice
            | true -> Lwt.return_unit)
          heads
    end
  end

  let copy t ~keep_max ~squash ~min ~max ~heads () =
    Log.debug (fun f -> f "copy");
    Lwt.catch
      (fun () ->
        (* Copy commits to lower: if squash then copy only the max commits *)
        ( if squash then Copy.CopyToLower.copy_max_commits t max
        else Copy.CopyToLower.copy t ~min ~max () )
        >>= fun () ->
        (* Copy max and heads after max to current_upper *)
        ( if keep_max then
          Copy.CopyToUpper.copy_max_commits t max >>= fun () ->
          Copy.CopyToUpper.copy_heads t max heads
        else Lwt.return_unit )
        >>= fun () ->
        (* Copy branches to both lower and current_upper *)
        Copy.copy_branches t >|= fun () -> Ok ())
      (function
        | Layered.Copy_error e -> Lwt.return_error (`Msg e)
        | e -> Fmt.kstrf Lwt.fail_invalid_arg "copy error: %a" Fmt.exn e)

  (** main thread takes the lock at the begining of freeze and async thread
      releases it at the end. This is to ensure that no two freezes can run
      simultaneously. *)
  let unsafe_freeze ~min ~max ~squash ~keep_max ~heads ~recovery ?hook t =
    Log.debug (fun l -> l "unsafe_freeze");
    ( if not recovery then
      Lwt_mutex.with_lock reset_lock (fun () -> X.Repo.flip_upper t)
    else Lwt.return_unit )
    >>= fun () ->
    (* Lwt.async (fun () -> *)
    (* Lwt.pause () >>= fun () -> *)
    ( may (fun f -> f `Before_Copy) hook >>= fun () ->
      copy t ~keep_max ~squash ~min ~max ~heads () >>= function
      | Ok () ->
          may (fun f -> f `Before_Clear) hook >>= fun () ->
          X.Repo.clear_previous_upper t >>= fun () ->
          Lwt_mutex.unlock freeze_lock;
          Log.debug (fun l -> l "free lock");
          may (fun f -> f `After_Clear) hook
      | Error (`Msg e) ->
          Fmt.kstrf Lwt.fail_with "[gc_store]: import error %s" e )
    >|= fun () -> Log.debug (fun l -> l "after async called to copy")

  let freeze_with_hook ?(min = []) ?(max = []) ?(squash = false) ?keep_max
      ?(heads = []) ?(recovery = false) ?hook t =
    let keep_max =
      match keep_max with None -> get_keep_max t.X.Repo.config | Some b -> b
    in
    (match max with [] -> Repo.heads t | m -> Lwt.return m) >>= fun max ->
    (match heads with [] -> Repo.heads t | m -> Lwt.return m) >>= fun heads ->
    Lwt_mutex.lock freeze_lock >>= fun () ->
    if t.X.Repo.closed then Lwt.fail_with "store is closed"
    else (
      Stats.freeze ();
      unsafe_freeze ~min ~max ~squash ~keep_max ~heads ~recovery ?hook t )

  let freeze = freeze_with_hook ?hook:None

  let layer_id = X.Repo.layer_id

  let async_freeze () = Lwt_mutex.is_locked freeze_lock

  let upper_in_use = X.Repo.upper_in_use

  let sync t = X.Repo.sync t

  module PrivateLayer = struct
    module Hook = struct
      type 'a t = 'a -> unit Lwt.t

      let v f = f
    end

    let wait_for_freeze () =
      Lwt_mutex.with_lock freeze_lock (fun () -> Lwt.return_unit)

    let freeze_with_hook = freeze_with_hook
  end
end

module Make
    (Config : CONFIG)
    (M : Irmin.Metadata.S)
    (C : Irmin.Contents.S)
    (P : Irmin.Path.S)
    (B : Irmin.Branch.S)
    (H : Irmin.Hash.S) =
struct
  module XNode = Irmin.Private.Node.Make (H) (P) (M)
  module XCommit = Irmin.Private.Commit.Make (H)
  include Make_ext (Config) (M) (C) (P) (B) (H) (XNode) (XCommit)
end
