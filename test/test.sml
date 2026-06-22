(* Tests for sml-transducer: map/filter/remove/take/drop/takeWhile/mapcat/cat/
   dedupe, composition (left-to-right), the runners transduce/into/xreduce,
   fusion into a single pass, and early termination of `take`. Reference values
   computed by hand and cross-checked against staged List.* pipelines. *)

structure Tests =
struct
  open Harness
  open Transducer

  fun isEven n = n mod 2 = 0
  val inc = fn x => x + 1
  fun add a b = a + b
  fun upto n = List.tabulate (n, fn i => i + 1)

  fun runAll () =
    let
      (* ------------------- single stages ------------------- *)
      val () = section "map / filter / remove"
      val () = checkIntList "map (+1)" ([2,3,4], into (map inc) [1,2,3])
      val () = checkIntList "filter even" ([2,4,6], into (filter isEven) (upto 6))
      val () = checkIntList "remove even" ([1,3,5], into (remove isEven) (upto 6))
      val () = checkIntList "map over []" ([], into (map inc) [])

      val () = section "take / drop / takeWhile"
      val () = checkIntList "take 3" ([1,2,3], into (take 3) (upto 10))
      val () = checkIntList "take 0" ([], into (take 0) [1,2,3])
      val () = checkIntList "take more than length" ([1,2,3], into (take 100) [1,2,3])
      val () = checkIntList "drop 2" ([3,4,5], into (drop 2) (upto 5))
      val () = checkIntList "drop 0" ([1,2,3], into (drop 0) [1,2,3])
      val () = checkIntList "drop all" ([], into (drop 10) [1,2,3])
      val () = checkIntList "takeWhile (<4)" ([1,2,3], into (takeWhile (fn n => n < 4)) [1,2,3,4,1,2])

      val () = section "mapcat / cat"
      val () = checkIntList "mapcat duplicate" ([1,1,2,2,3,3], into (mapcat (fn x => [x,x])) [1,2,3])
      val () = checkIntList "mapcat filter-ish"
                 ([2,4,6], into (mapcat (fn x => if isEven x then [x] else [])) (upto 6))
      val () = checkIntList "cat flattens" ([1,2,3,4,5], into cat [[1,2],[3],[4,5]])
      val () = checkIntList "cat of []" ([], into cat ([] : int list list))

      val () = section "dedupe (consecutive)"
      val () = checkIntList "dedupe runs" ([1,2,3,1], into (dedupe (op =)) [1,1,2,2,2,3,1,1])
      val () = checkIntList "dedupe no dups" ([1,2,3], into (dedupe (op =)) [1,2,3])
      val () = checkIntList "dedupe all same" ([5], into (dedupe (op =)) [5,5,5])
      val () = checkIntList "dedupe empty" ([], into (dedupe (op =)) ([] : int list))

      (* ------------------- composition ------------------- *)
      val () = section "comp (left-to-right data order)"
      val () = checkIntList "map then filter"
                 ([2,4,6], into (comp (map inc) (filter isEven)) (upto 6))
      val () = checkIntList "map*2 then filter>4"
                 ([6,8], into (comp (map (fn x => x * 2)) (filter (fn n => n > 4))) [1,2,3,4])
      val () = checkIntList "filter>4 then map*2 (order matters)"
                 ([], into (comp (filter (fn n => n > 4)) (map (fn x => x * 2))) [1,2,3,4])
      val () = checkIntList "compList [map,filter,take]"
                 ([2,4], into (compList [map inc, filter isEven, take 2]) (upto 100))
      val () = checkIntList "identity" ([1,2,3], into identity [1,2,3])

      (* ------------------- fusion: map+filter+take in one pass ------------------- *)
      val () = section "fusion: (map o filter o take) one pass"
      fun staged xs = List.take (List.filter isEven (List.map inc xs), 3)
      val pipe = comp (map inc) (comp (filter isEven) (take 3))
      val () = checkIntList "fused = [2,4,6]" ([2,4,6], into pipe (upto 100))
      val () = checkIntList "fused = staged list pipeline" (staged (upto 100), into pipe (upto 100))

      val () = section "fusion: early termination stops processing"
      val cnt = ref 0
      val incCount = fn x => (cnt := !cnt + 1; x + 1)
      val r = into (comp (map incCount) (comp (filter isEven) (take 3))) (upto 100)
      val () = checkIntList "early-stop result" ([2,4,6], r)
      (* map runs on inputs 1..5 only (3rd even produced at input 5), not all 100 *)
      val () = checkInt "map invoked exactly 5 times (single pass + early stop)" (5, !cnt)
      (* takeWhile also stops early *)
      val cnt2 = ref 0
      val _ = into (comp (map (fn x => (cnt2 := !cnt2 + 1; x))) (takeWhile (fn n => n < 4))) (upto 100)
      val () = checkInt "takeWhile stops after first failure" (4, !cnt2)

      (* ------------------- runners: transduce / xreduce ------------------- *)
      val () = section "transduce (fold with a reducer)"
      val () = checkInt "transduce identity sum [1..10]" (55, transduce identity add 0 (upto 10))
      val () = checkInt "transduce filter even sum" (30, transduce (filter isEven) add 0 (upto 10))
      val () = checkInt "transduce map sq + filter>10 sum"
                 (41, transduce (comp (map (fn x => x * x)) (filter (fn n => n > 10)))
                                add 0 [1,2,3,4,5])
      val () = checkInt "transduce take 3 sum (early stop)" (6, transduce (take 3) add 0 (upto 100))
      val () = checkInt "transduce mapcat sum"
                 (12, transduce (mapcat (fn x => [x,x])) add 0 [1,2,3])

      val () = section "xreduce (low-level status-aware fold)"
      val () = checkInt "xreduce no stop sums" (6, xreduce (fn acc => fn a => More (acc + a)) 0 [1,2,3])
      val () = checkInt "xreduce Stop short-circuits"
                 (6, xreduce (fn acc => fn a => if a > 3 then Stop acc else More (acc + a)) 0 [1,2,3,4,5])
      val () = checkInt "xreduce empty" (0, xreduce (fn acc => fn a => More (acc + a)) 0 [])
    in
      Harness.run ()
    end

  val run = runAll
end
