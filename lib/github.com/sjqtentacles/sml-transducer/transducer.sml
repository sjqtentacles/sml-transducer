(* transducer.sml - reducing-function transformers with fusion + early stop.

   Each transducer is `fn rf => fn acc => fn a => ...`: given the downstream
   reducer `rf` it returns a new reducer over the upstream element type. `comp`
   is plain function plumbing (`xf1 (xf2 rf)`), so a composed pipeline does all
   its per-element work in one pass with no intermediate lists. Stateful stages
   allocate their `ref` when applied to `rf`, i.e. once per run. *)

structure Transducer :> TRANSDUCER =
struct

  datatype 'r status = More of 'r | Stop of 'r

  type ('r, 'a, 'b) xform = ('r -> 'b -> 'r status) -> ('r -> 'a -> 'r status)

  (* ---- transducers ---- *)
  fun map f = fn rf => fn acc => fn a => rf acc (f a)

  fun filter p = fn rf => fn acc => fn a => if p a then rf acc a else More acc
  fun remove p = filter (fn a => not (p a))

  fun take n =
    fn rf =>
      let val c = ref 0
      in
        fn acc => fn a =>
          if !c >= n then Stop acc
          else
            let
              val () = c := !c + 1
              val res = rf acc a
            in
              if !c >= n
              then (case res of More r => Stop r | s => s)
              else res
            end
      end

  fun drop n =
    fn rf =>
      let val c = ref 0
      in
        fn acc => fn a =>
          if !c < n then (c := !c + 1; More acc) else rf acc a
      end

  fun takeWhile p =
    fn rf => fn acc => fn a => if p a then rf acc a else Stop acc

  fun mapcat f =
    fn rf => fn acc => fn a =>
      let
        fun go acc [] = More acc
          | go acc (x :: xs) =
              (case rf acc x of
                   More r => go r xs
                 | Stop r => Stop r)
      in go acc (f a) end

  val cat = fn rf => mapcat (fn x => x) rf

  fun dedupe eq =
    fn rf =>
      let val last = ref NONE
      in
        fn acc => fn a =>
          case !last of
              SOME p => if eq (p, a) then More acc
                        else (last := SOME a; rf acc a)
            | NONE => (last := SOME a; rf acc a)
      end

  (* ---- composition ---- *)
  fun identity rf = rf
  fun comp xf1 xf2 = fn rf => xf1 (xf2 rf)
  fun compList xfs = List.foldr (fn (x, acc) => comp x acc) identity xfs

  (* ---- runners ---- *)
  fun xreduce step init coll =
    let
      fun loop acc [] = acc
        | loop acc (x :: xs) =
            (case step acc x of
                 More r => loop r xs
               | Stop r => r)
    in loop init coll end

  fun transduce xf rf init coll =
    xreduce (xf (fn acc => fn b => More (rf acc b))) init coll

  fun into xf coll =
    List.rev (transduce xf (fn acc => fn b => b :: acc) [] coll)
end
