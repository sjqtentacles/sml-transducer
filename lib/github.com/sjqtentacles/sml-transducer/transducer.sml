(* transducer.sml - reducing-function transformers with fusion + early stop.

   Each transducer is `fn rf => fn acc => fn a => ...`: given the downstream
   reducer `rf` it returns a new reducer over the upstream element type. `comp`
   is plain function plumbing (`xf1 (xf2 rf)`), so a composed pipeline does all
   its per-element work in one pass with no intermediate lists. Stateful stages
   allocate their `ref` when applied to `rf`, i.e. once per run. *)

structure Transducer :> TRANSDUCER =
struct

  datatype 'r status = More of 'r | Stop of 'r

  type ('r, 'b) reducer = { step : 'r -> 'b -> 'r status, complete : 'r -> 'r }

  type ('r, 'a, 'b) xform = ('r, 'b) reducer -> ('r, 'a) reducer

  fun reducer step = { step = step, complete = fn r => r }
  fun completing step complete = { step = step, complete = complete }

  (* Explicitly-typed field accessors so we never need flexible record patterns
     (which MLton refuses to resolve when the element type is ambiguous). *)
  fun rstep (rf : ('r, 'b) reducer) = #step rf
  fun rdone (rf : ('r, 'b) reducer) = #complete rf

  fun unStatus (More r) = r
    | unStatus (Stop r) = r

  (* ---- transducers ---- *)
  fun map f =
    fn (rf : ('r, 'b) reducer) =>
      { step = fn acc => fn a => rstep rf acc (f a), complete = rdone rf }

  fun mapIndexed f =
    fn rf =>
      let val i = ref 0
      in
        { step = fn acc => fn a =>
            let val n = !i in (i := n + 1; rstep rf acc (f n a)) end
        , complete = rdone rf }
      end

  fun filter p =
    fn rf =>
      { step = fn acc => fn a => if p a then rstep rf acc a else More acc
      , complete = rdone rf }

  fun remove p = filter (fn a => not (p a))

  fun keep f =
    fn rf =>
      { step = fn acc => fn a => (case f a of SOME b => rstep rf acc b | NONE => More acc)
      , complete = rdone rf }

  fun keepIndexed f =
    fn rf =>
      let val i = ref 0
      in
        { step = fn acc => fn a =>
            let val n = !i
            in (i := n + 1; case f n a of SOME b => rstep rf acc b | NONE => More acc)
            end
        , complete = rdone rf }
      end

  fun take n =
    fn rf =>
      let val c = ref 0
      in
        { step = fn acc => fn a =>
            if !c >= n then Stop acc
            else
              let
                val () = c := !c + 1
                val res = rstep rf acc a
              in
                if !c >= n
                then (case res of More r => Stop r | s => s)
                else res
              end
        , complete = rdone rf }
      end

  fun takeNth n =
    fn rf =>
      let val c = ref 0
      in
        { step = fn acc => fn a =>
            let val k = !c
            in (c := k + 1;
                if n > 0 andalso k mod n = 0 then rstep rf acc a else More acc)
            end
        , complete = rdone rf }
      end

  fun drop n =
    fn rf =>
      let val c = ref 0
      in
        { step = fn acc => fn a =>
            if !c < n then (c := !c + 1; More acc) else rstep rf acc a
        , complete = rdone rf }
      end

  fun takeWhile p =
    fn rf =>
      { step = fn acc => fn a => if p a then rstep rf acc a else Stop acc
      , complete = rdone rf }

  fun dropWhile p =
    fn rf =>
      let val dropping = ref true
      in
        { step = fn acc => fn a =>
            if !dropping andalso p a then More acc
            else (dropping := false; rstep rf acc a)
        , complete = rdone rf }
      end

  fun interpose sep =
    fn rf =>
      let val started = ref false
      in
        { step = fn acc => fn a =>
            if !started
            then (case rstep rf acc sep of
                      More r => rstep rf r a
                    | Stop r => Stop r)
            else (started := true; rstep rf acc a)
        , complete = rdone rf }
      end

  fun mapcat f =
    fn rf =>
      { step = fn acc => fn a =>
          let
            fun go acc [] = More acc
              | go acc (x :: xs) =
                  (case rstep rf acc x of
                       More r => go r xs
                     | Stop r => Stop r)
          in go acc (f a) end
      , complete = rdone rf }

  fun cat rf = mapcat (fn x => x) rf

  fun dedupe eq =
    fn rf =>
      let val last = ref NONE
      in
        { step = fn acc => fn a =>
            (case !last of
                 SOME p => if eq (p, a) then More acc
                           else (last := SOME a; rstep rf acc a)
               | NONE => (last := SOME a; rstep rf acc a))
        , complete = rdone rf }
      end

  fun distinct eq =
    fn rf =>
      let val seen = ref []
      in
        { step = fn acc => fn a =>
            if List.exists (fn x => eq (x, a)) (!seen) then More acc
            else (seen := a :: !seen; rstep rf acc a)
        , complete = rdone rf }
      end

  fun partitionAll n =
    fn (rf : ('r, 'a list) reducer) =>
      let val buf = ref ([] : 'a list)
      in
        { step = fn acc => fn a =>
            let
              val cur = !buf @ [a]
            in
              if List.length cur >= n
              then (buf := []; rstep rf acc cur)
              else (buf := cur; More acc)
            end
        , complete = fn acc =>
            (case !buf of
                 [] => rdone rf acc
               | part => (buf := []; rdone rf (unStatus (rstep rf acc part)))) }
      end

  (* ---- composition ---- *)
  fun identity rf = rf
  fun comp xf1 xf2 = fn rf => xf1 (xf2 rf)
  fun compList xfs = List.foldr (fn (x, acc) => comp x acc) identity xfs

  (* ---- runners ---- *)
  fun xreduce (rf : ('r, 'a) reducer) init coll =
    let
      fun loop acc [] = rdone rf acc
        | loop acc (x :: xs) =
            (case rstep rf acc x of
                 More r => loop r xs
               | Stop r => rdone rf r)
    in loop init coll end

  fun transduce xf rf init coll =
    xreduce (xf (reducer (fn acc => fn b => More (rf acc b)))) init coll

  fun into xf coll =
    List.rev (transduce xf (fn acc => fn b => b :: acc) [] coll)

  fun intoString xf coll =
    transduce xf (fn acc => fn (b : string) => acc ^ b) "" coll

  fun intoArray xf coll =
    Vector.fromList (into xf coll)
end
