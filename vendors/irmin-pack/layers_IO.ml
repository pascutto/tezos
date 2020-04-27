let src = Logs.Src.create "irmin.layers.io" ~doc:"IO for irmin-layers"

module Log = (val Logs.src_log src : Logs.LOG)

module type S = sig
  type t

  val v : string -> bytes -> t

  val write : t -> bytes -> unit

  val read : t -> bytes -> unit

  val close : t -> unit

  val read_flip_file : t -> bool

  val write_flip_file : bool -> t -> unit
end

module Unix : S = struct
  type t = { file : string; fd : Unix.file_descr }

  let write t buf =
    let off = Unix.lseek t.fd 0 Unix.SEEK_SET in
    assert (off = 0);
    let len = Bytes.length buf in
    let n = Unix.write t.fd buf 0 len in
    assert (n = len)

  let read t buf =
    let off = Unix.lseek t.fd 0 Unix.SEEK_SET in
    assert (off = 0);
    let n = Unix.read t.fd buf 0 1 in
    assert (n = 1)

  let close t = Unix.close t.fd

  let v file init =
    match Sys.file_exists file with
    | false ->
        let fd = Unix.openfile file Unix.[ O_CREAT; O_RDWR; O_CLOEXEC ] 0o644 in
        let t = { file; fd } in
        write t init;
        t
    | true ->
        let fd = Unix.openfile file Unix.[ O_EXCL; O_RDWR; O_CLOEXEC ] 0o644 in
        { file; fd }

  let read_file file =
    let buf = Bytes.create 1 in
    read file buf;
    let ch = Bytes.get buf 0 in
    int_of_char ch

  let read_flip_file file =
    match read_file file with
    | 0 -> false
    | 1 -> true
    | d -> failwith ("corrupted flip file " ^ string_of_int d)

  let write_file file i =
    let buf = Bytes.make 1 (char_of_int i) in
    write file buf

  let write_flip_file flip flip_file =
    let flip = if flip then 1 else 0 in
    write_file flip_file flip
end
