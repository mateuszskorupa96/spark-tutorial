from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("word count")
    sc = SparkContext(conf = conf)
    
    lines = sc.textFile("/home/osboxes/Downloads/CODES/python-spark-tutorial-master/in/word_count.text")
    
    words = lines.flatMap(lambda line: line.split(" "))
    
    wordCounts = words.countByValue()
    
    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))

