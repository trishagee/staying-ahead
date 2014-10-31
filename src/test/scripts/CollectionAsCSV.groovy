import com.mongodb.MongoClient

def mongoClient = new MongoClient();
def collection = mongoClient.getDB("Cafelito").getCollection("CoffeeShop")

def numberOfCoffeeShops = collection.find().count()

def fields = new TreeSet<String>()
def table = new HashMap<String, List<String>>()
def allCoffeeShops = collection.find().toArray()
allCoffeeShops.eachWithIndex { coffeeShop, i ->
    def coffeeShopFields = coffeeShop.keySet()
    fields.addAll(coffeeShopFields)
    for(field in coffeeShopFields) {
        table.putIfAbsent(field, new ArrayList<String>())
        def values = table.get(field)
        def startIndex = values.size()
        for (j = startIndex; j<i; j++)
            values.add("")
        values.add(coffeeShop.get(field))
    }
    println coffeeShop
}

for (field in fields) {
    print "${field},"
}

for (field in table.keySet()) {
    println "${field}: ${table.get(field)}" 
}

new File("output.csv").withWriter { out ->
    table.each() { key, value ->
        out.writeLine("${key}=${value}")
    }
}