import com.mongodb.client.MongoClients;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class BigData {

	public static void main(String[] args) {
		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://kalin:123@diplomdb.0cikk.mongodb.net/myFirstDatabase?retryWrites=true&w=majority");
		Scanner s = new Scanner(System.in);

		List<Object> databases = mongoClient.listDatabaseNames().into(new ArrayList<Object>());
		System.out.println("Бази данни:");
		System.out.println(databases);
		System.out.println("Избери база данни");
		String dbname = s.nextLine();
		MongoDatabase Database = mongoClient.getDatabase(dbname);

		System.out.println("Колекции в база данни - " + dbname + ":");
		List<Object> colls = Database.listCollectionNames().into(new ArrayList<Object>());
		System.out.println(colls);

		System.out.println("Избери колекция");
		String collname = s.nextLine();
		MongoCollection<Document> coll = Database.getCollection(collname);

		System.out.println("Избери действие:\nfilter - извежда всички документи отговарящи на определен критерий"
				+ "\navg - намиране на средна стойност" + "\nsum -  сумиране на стойности"
				+ "\nminmax - намиране на минимална или максимална стойност");
		String p = s.nextLine();

		if (p.equals("filter")) {
			calculateFilter(s, coll);
		}

		if (p.equals("avg")) {
			calculateAverage(s, coll);
		}

		if (p.equals("sum")) {
			calculateSum(s, coll);
		}

		if (p.equals("minmax")) {
			calculateMinMax(s, coll);
		}
	}

	private static void printResult(Iterator<Document> it) {
		while (it.hasNext()) {
			System.out.println(it.next());
		}
	}

	private static void calculateFilter(Scanner s, MongoCollection<Document> coll) {
		System.out.println("Име на поле");
		String key = s.nextLine();

		System.out.println("Неговата стойност");
		String value = s.nextLine();

		Bson filter = (Filters.eq(key, value));

		Iterator<Document> it = coll.find(filter).iterator();
		printResult(it);
	}

	private static void calculateAverage(Scanner s, MongoCollection<Document> coll) {
		System.out.println("Ключ");
		String key = s.nextLine();

		System.out.println("Стойност");
		String value = s.nextLine();

		String mapFunction = String.format("function() { emit(this.%s, this.%s) }", key, value);
		String reduceFunction = String.format("function(%s, %s) { return Array.avg(%s); }", key, value, value);

		MapReduceIterable<Document> mapred = coll.mapReduce(mapFunction, reduceFunction);
		Iterator<Document> it = mapred.iterator();
		printResult(it);
	}

	private static void calculateSum(Scanner s, MongoCollection<Document> coll) {
		System.out.println("Ключ");
		String key = s.nextLine();

		System.out.println("Стойност");
		String value = s.nextLine();

		String mapFunction = String.format("function() { emit(this.%s, this.%s) }", key, value);
		String reduceFunction = String.format("function(%s, %s) { return Array.sum(%s); }", key, value, value);

		MapReduceIterable<Document> mapred = coll.mapReduce(mapFunction, reduceFunction);
		Iterator<Document> it = mapred.iterator();
		printResult(it);
	}

	private static void calculateMinMax(Scanner s, MongoCollection<Document> coll) {
		System.out.println("Ключ");
		String key = s.nextLine();

		System.out.println("Стойност");
		String value = s.nextLine();
		System.out.println("min или max");
		String process = s.nextLine();

		String mapFunction = String.format("function() { emit(this.%s, this.%s) }", key, value);
		String reduceFunction = String.format("function(%s,%s){ return Math.%s.apply(Math,%s); }", key, value, process,
				value);

		MapReduceIterable<Document> mapred = coll.mapReduce(mapFunction, reduceFunction);
		Iterator<Document> it = mapred.iterator();
		printResult(it);
	}
}
