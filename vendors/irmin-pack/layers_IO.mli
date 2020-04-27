module type S = sig
  type t

  val v : string -> bytes -> t

  val write : t -> bytes -> unit

  val read : t -> bytes -> unit

  val close : t -> unit

  val read_flip_file : t -> bool

  val write_flip_file : bool -> t -> unit
end

module Unix : S
