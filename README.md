# NTHU_MDA_HW1

環境

    Pyspark
    
方法

    step 1  將 M N 矩陣 透過 map function 轉成 key-value pairs
             M : Mij => (j,(i,Mij))
             N : Njk => (j,(k,Njk))

    step 2  Join M 跟 N 的 key-value pair :
            (j,(i,Mji)) join (j,(k,Njk)) => (j,((i,Mij),(k,Njk)))

    step 3  將step2後的key-value pairs 透過map function 成 （(i,k), v) 其中 v = Mij*Njk

    step 4  Reduce the key-value pairs, 將相同的 key 的 v 總和

    step 5  針對 key 排列所有的 pairs (row to column)

    step 6  輸出成 output.txt

執行

    python3 HW1.py
    