#Acumuladores en pyspark
from pyspark import SparkConf, SparkContext
onf = SparkConf().setMaster("local").setAppName("Mi programa")
sc = SparkCOntext(conf = conf)

lines = sc.textFile("Ejemplo1.txt")

py = sc.accuulator(0)
sp = sc.accuulator(0)

def lenguajes(linea):
    golbal py, sp
    if "python" in linea:
        py += 1
        if "spark" in linea:
            sp += 1
        return True
    if "spark" in linea:
        sp += 1
        return True
    else:
        return False

valores = lines.filter(lenguajes)

py

sp
