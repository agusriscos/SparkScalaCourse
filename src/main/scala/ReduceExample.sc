val my_list = List(1, 2, 3, 4, 5, 6, 7)

my_list.reduce((x, y) => x.min(y))

my_list.reduce((x, y) => x.max(y))

my_list.product

my_list.reduce((x, y) => x - y)


val other_list = List(("Z", 1),("A", 20),("B", 70),("C", 40),("B", 30),("B", 60))

println("output max : "+ other_list.reduce( (a, b) => ("max", a._2 min b._2))._2)
println("output max : "+ other_list.reduce( (a, b) => ("max", a._2 max b._2))._2)
println("output sum : "+ other_list.reduce( (a, b) => ("Sum", a._2 + b._2))._2)
