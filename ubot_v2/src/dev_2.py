import store_data_4
import bdd


con1 = bdd.MyBdd()
#con1.useBdd("root", "123azerty")
con2 = bdd.MyBdd()
con3 = bdd.MyBdd()
con4 = bdd.MyBdd()
con5 = bdd.MyBdd()
con6 = bdd.MyBdd()
con7 = bdd.MyBdd()
con8 = bdd.MyBdd()
con9 = bdd.MyBdd()
con10 = bdd.MyBdd()
con11 = bdd.MyBdd()
con12 = bdd.MyBdd()
con13 = bdd.MyBdd()
con14 = bdd.MyBdd()
con15 = bdd.MyBdd()
con16 = bdd.MyBdd()
con17 = bdd.MyBdd()

new_update = store_data_4.New_update(con1, con2, con3, con4, con5,
                                     con6, con7, con8, con9, con10,
                                      con11, con12, con13, con14, con15,
                                      con16, con17)

new_update.send_rank(["other"])


