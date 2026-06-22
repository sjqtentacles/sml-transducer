(* demo.sml - composable, fused transducers: a single pass that maps, filters
   and takes, plus mapcat/dedupe and the reducing runners. The step counter
   shows that composition fuses and `take` terminates early. Deterministic:
   identical output on every run and both compilers. *)

open Transducer

fun ints xs = "[" ^ String.concatWith "," (List.map Int.toString xs) ^ "]"
fun upto n = List.tabulate (n, fn i => i + 1)
fun isEven n = n mod 2 = 0
fun add a b = a + b

val () = print "Fused pipeline over [1..100]:  map (+1) |> filter even |> take 3\n"
val steps = ref 0
val incCount = fn x => (steps := !steps + 1; x + 1)
val out = into (comp (map incCount) (comp (filter isEven) (take 3))) (upto 100)
val () = print ("  result            = " ^ ints out ^ "\n")
val () = print ("  source elements processed = " ^ Int.toString (!steps)
                ^ " (not 100 - single pass, early stop)\n")

val () = print "\nmapcat / cat / dedupe:\n"
val () = print ("  mapcat (fn x=>[x,x]) [1,2,3] = "
                ^ ints (into (mapcat (fn x => [x, x])) [1,2,3]) ^ "\n")
val () = print ("  cat [[1,2],[3],[4,5]]        = "
                ^ ints (into cat [[1,2],[3],[4,5]]) ^ "\n")
val () = print ("  dedupe [1,1,2,2,2,3,1,1]     = "
                ^ ints (into (dedupe (op =)) [1,1,2,2,2,3,1,1]) ^ "\n")

val () = print "\nReducing runners:\n"
val () = print ("  transduce (filter even) (+) over [1..10] = "
                ^ Int.toString (transduce (filter isEven) add 0 (upto 10)) ^ "\n")
val () = print ("  transduce (take 3) (+) over [1..100]     = "
                ^ Int.toString (transduce (take 3) add 0 (upto 100))
                ^ " (early stop)\n")
