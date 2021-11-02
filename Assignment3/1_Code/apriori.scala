import java.io._
import scala.io.Source
import scala.util.control.Breaks._
import collection.mutable.Map
import collection.mutable.SortedSet

object apriori {
    val support = 771;

    def convert_set_to_string(set:SortedSet[String]):String = {
        var ans:String = "";
        for(x <- set) {
            ans += x + ";";
        }
        return ans.slice(0, ans.length()-1);
    }
    def eliminate_items(Ck:Map[String, Int]):Map[String, Int] = {
        var temp:Map[String, Int] = Ck.clone();
        var to_be_eliminated:Vector[String] = Vector();
        for((k, v) <- Ck) {
            if(v < support) {
                to_be_eliminated = to_be_eliminated :+ k;
            }
        }
        to_be_eliminated.foreach(x => temp -= x);
        return temp;
    }
    def join(set:Map[String, Int], len:Int):Map[String, Int] = {
        var item_set:Map[String, Int] = Map();
        for((k, v) <- set) {
            var items:Array[String] = k.split(';');
            var temp1:SortedSet[String] = SortedSet();
            for(item <- items) {
                temp1 += item;
            }
            for((x, y) <- set) {
                var new_items:Array[String] = x.split(';');
                var temp3:SortedSet[String] = SortedSet();
                for(new_item <- new_items) {
                    temp3 += new_item;
                }
                var temp4 = temp1 ++ temp3;
                if(len == temp4.size) {
                    item_set += (convert_set_to_string(temp4) -> 0);
                }
            }
        }
        return item_set;
    }
    def generate_subsets(str:Array[String], len:Int):Array[String] = {
        var n:Int = str.length;
        var subsets:Array[String] = Array();
        for(i <- 0 to n-1) {
            var sortedSet:SortedSet[String] = SortedSet();
            if(i == 0) {
                for(j <- 1 to n-1) sortedSet += str(j);
            } else if(i == n-1) {
                for(j <- 0 to n-2) sortedSet += str(j);
            } else {
                for(j <- 0 to i-1) sortedSet += str(j);
                for(j <- i+1 to n-1) sortedSet += str(j);
            }
            subsets = subsets :+ convert_set_to_string(sortedSet);
        }
        return subsets;
    }
    def prune(Ck_plus_1:Map[String, Int], Lk:Map[String, Int], len:Int):Unit = {
        var to_be_eliminated:Vector[String] = Vector();
        for((k, v) <- Ck_plus_1) {
            var temp1:Array[String] = generate_subsets(k.split(';'), len);
            var fl:Int = 0;
            breakable {
                for(x <- temp1) {
                    if(!Lk.contains(x) || (Lk(x) < support)) {
                        fl = 1;
                        break;
                    }
                }
            }
            if(fl == 1) {
                to_be_eliminated = to_be_eliminated :+ k;
            }
        }
        to_be_eliminated.foreach(x => Ck_plus_1 -= x)
    }
    def find_support(Ck:Map[String, Int], hash_items:Vector[Map[String, Int]]):Unit = {
        for((k, v) <- Ck) {
            var cnt:Int = 0;
            var temp1:Array[String] = k.split(';');
            for(x <- hash_items) {
                var fl:Int = 0;
                breakable {
                    for(y <- temp1) {
                        if(!x.contains(y)) {
                            fl = 1;
                            break;
                        }
                    }
                }
                if(fl != 1) {
                    cnt += 1;
                }
            }
            Ck(k) = cnt;
        }
    }

    def main(args: Array[String]) {
        // Reading file and creating items
        val source = scala.io.Source.fromFile("./categories.txt.txt");
        val file = try source.mkString finally source.close();

        var lines:Array[String] = file.split("\r\n");
        var Ck, Lk:Map[String, Int] = Map();
        var C, L:Map[Int, Map[String, Int]] = Map();
        var hash_items_per_line:Vector[Map[String, Int]] = Vector();

        // Creating one item itemset and at the same time hashing the items per line
        // Ck <-- k item candidate set
        // Lk <-- k item set greater than support value
        // hash_items_per_line <-- vector of hash of items per line for all lines
        for(line <- lines) {
            var items:Array[String] = line.split(';');
            var temp:Map[String, Int] = Map();
            for(item <- items) {
                temp += (item -> 1);
                if(Ck.contains(item)) {
                    Ck(item) += 1;
                } else {
                    Ck += (item -> 1);
                }
            }
            hash_items_per_line = hash_items_per_line :+ temp;
        }
        var iteration:Int = 1;
        C += (iteration -> Ck);
        Lk = eliminate_items(Ck);
        L += (iteration -> Lk);
        while(Lk.size > 1) {
            iteration += 1;
            // Step 1 - Join step. Generates K+1 itemset from K itemsets
            Ck = join(Lk, iteration);
            /* Step 2 - Prunes the itemset such that every subset of the itemset
               should be greater than support */
            prune(Ck, Lk, iteration - 1);
            // Step 3 - Finds the support value for the pruned itemset
            find_support(Ck, hash_items_per_line);
            // Step 4 - Eliminats / Removes itemset that has support value < 771
            Lk = eliminate_items(Ck);
            C += (iteration -> Ck);
            L += (iteration -> Lk);
        }
        
        // Writing to pattern_1.txt for case 1
        var writer = new PrintWriter(new File("pattern_1.txt"));
        for((k, v) <- L(1)) {
            var str:String = v + ":" + k + "\n";
            writer.write(str);
        }
        writer.close();

        // Writing to pattern_2.txt for case 2
        writer = new PrintWriter(new File("pattern_2.txt"));
        for(i <- 1 to iteration) {
            if(L(i).size > 0) {
                for((k, v) <- L(i)) {
                    var str:String = v + ":" + k + "\n";
                    writer.write(str);
                }
            }
        }
        writer.close();
    }
}