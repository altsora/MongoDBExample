import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public final class Business {
    private static MongoClient mongoClient;
    private static MongoCollection<Document> shops;
    private static MongoCollection<Document> items;
    //==================================================================
    private static final String DATABASE_NAME = "store";
    private static final String COLLECTION_SHOPS = "shops";
    private static final String COLLECTION_ITEMS = "items";
    //==================================================================
    private static final String FIELD_SHOP_NAME = "shop";
    private static final String FIELD_SHOP_PRODUCTS = "products";
    private static final String FIELD_ITEM_NAME = "item";
    private static final String FIELD_ITEM_PRICE = "price";
    //==================================================================
    private static final String ADD_SHOP = "adds";
    private static final String ADD_ITEM = "addi";
    private static final String CLEAR = "clear";
    private static final String PURCHASE = "pur";
    private static final String EXIT = "exit";
    private static final String SHOW_INFO = "show";
    //==================================================================

    public static void init() {
        mongoClient = new MongoClient();
        MongoDatabase mongoDatabase = mongoClient.getDatabase(DATABASE_NAME);
        items = mongoDatabase.getCollection(COLLECTION_ITEMS);
        shops = mongoDatabase.getCollection(COLLECTION_SHOPS);
    }

    public static void control() throws InterruptedException {
        Thread.sleep(1000);
        commands();

        Scanner scanner = new Scanner(System.in);
        System.out.println("Введите команду:");
        String command = scanner.nextLine();

        while (!command.equals(EXIT) && !command.isEmpty()) {
            String[] parameters = command.split(" ");
            int words = parameters.length;
            switch (parameters[0]) {
                case CLEAR:
                    if (words > 2) clear(parameters[1]);
                    else clearAll();
                    break;
                case ADD_SHOP:
                    if (words == 1) System.out.println("Не указано название магазина.");
                    else addShop(parameters[1]);
                    break;
                case ADD_ITEM:
                    if (words == 3) {
                        try {
                            int price = Integer.parseInt(parameters[2]);
                            String item = parameters[1];
                            addItem(item, price);
                        } catch (NumberFormatException ex) {
                            System.out.println("Цена должна быть обозначена целым числом.");
                        }
                    } else System.out.println("Формат команды: " + ADD_ITEM + " ITEM PRICE");
                    break;
                case PURCHASE:
                    if (words == 3) {
                        String item = parameters[1];
                        String shop = parameters[2];
                        addItemInShop(item, shop);
                    } else System.out.println("Формат команды: " + PURCHASE + " ITEM SHOP");
                    break;
                case SHOW_INFO:
                    showInfo();
                    break;
                default:
                    commands();
            }

            System.out.println("\nВведите команду:");
            command = scanner.nextLine();
        }
        System.out.println("Сервис закрыт.");
    }

    private static void commands() {
        System.out.println("===СПИСОК КОМАНД===");
        System.out.println("\t" + ADD_SHOP + " SHOP");
        System.out.println("\t" + ADD_ITEM + " ITEM PRICE");
        System.out.println("\t" + CLEAR + " [-s|-i|-a]");
        System.out.println("\t" + PURCHASE + " ITEM SHOP");
        System.out.println("\t" + SHOW_INFO);
        System.out.println("\t" + EXIT);
        System.out.println("===================");
    }

    public static void addShop(String shop) {
        Document newShop = new Document()
                .append(FIELD_SHOP_NAME, shop)
                .append(FIELD_SHOP_PRODUCTS, new ArrayList<String>());
        boolean shopExists = (shops.find(new Document().append(FIELD_SHOP_NAME, shop)).first() != null);
        if (shopExists) {
            System.out.printf("Магазин \"%s\" уже существует.%n", shop);
        } else {
            shops.insertOne(newShop);
            System.out.printf("Магазин \"%s\" успешно добавлен.%n", shop);
        }
    }

    public static void addItem(String item, int price) {
        Document newItem = new Document()
                .append(FIELD_ITEM_NAME, item)
                .append(FIELD_ITEM_PRICE, price);
        boolean itemExists = (items.find(new Document().append(FIELD_ITEM_NAME, item)).first() != null);
        if (itemExists) {
            System.out.printf("Товар \"%s\" уже существует.%n", item);
        } else {
            items.insertOne(newItem);
            System.out.printf("Товар \"%s\" по цене %d успешно добавлен.%n", item, price);
        }
    }

    public static void addItemInShop(String item, String shop) {
        Document existingShop = shops.find(new Document(FIELD_SHOP_NAME, shop)).first();
        Document existingItem = items.find(new Document(FIELD_ITEM_NAME, item)).first();
        if (existingShop == null) {
            System.out.printf("Магазин \"%s\" не существует.%n", shop);
            return;
        }
        if (existingItem == null) {
            System.out.printf("Товар \"%s\" не существует.%n", item);
            return;
        }

        Document existingProduct = shops
                .find(new Document(FIELD_SHOP_NAME, shop).append(FIELD_SHOP_PRODUCTS, item))
                .first();
        if (existingProduct != null) {
            System.out.printf("Товар \"%s\" уже есть в магазине \"%s\".%n", item, shop);
            return;
        }
        Document updatedShop = new Document("$push", new Document(FIELD_SHOP_PRODUCTS, item));
        shops.updateOne(existingShop, updatedShop);
        System.out.printf("В магазин \"%s\" добавлен товар \"%s\".%n", shop, item);
    }

    public static void showShops() {
        int i = 0;
        System.out.println("===МАГАЗИНЫ===");
        for (Document document : shops.find()) {
            List<String> products = (List<String>) document.get(FIELD_SHOP_PRODUCTS);
            System.out.println(++i + ".\tМагазин: " + document.getString(FIELD_SHOP_NAME));
            System.out.println("\t\tПродуктов: " + products.size());
            System.out.println("\t\tПродукты: " + products);
        }
        System.out.println("==============");
    }

    public static void showItems() {
        int i = 0;
        System.out.println("====ТОВАРЫ====");
        for (Document document : items.find()) {
            System.out.println(++i + ".\tПродукт: " + document.getString(FIELD_ITEM_NAME));
            System.out.println("\t\tЦена:  " + document.getInteger(FIELD_ITEM_PRICE));
        }
        System.out.println("==============");
    }

    public static void showInfo() {
        System.out.println("\n*\t*\t*\tОБЩАЯ СТАТИСТИКА\t*\t*\t*\n");
        String alias = "result";
        String countProducts = "countProducts";
        String maxPrice = "maxPrice";
        String avgPrice = "avgPrice";
        String minPrice = "minPrice";
        String priceLt100 = "priceLt100";
        String fieldName = "fieldName";

        Bson lookup = Aggregates.lookup(COLLECTION_ITEMS, FIELD_SHOP_PRODUCTS, FIELD_ITEM_NAME, alias);
        Bson unwind = Aggregates.unwind("$" + alias);
        Bson field = Aggregates.addFields(new Field<>(
                fieldName,
                new Document("$cond",
                        new Document("if",
                                new Document("$lt", Arrays.asList("$" + alias + "." + FIELD_ITEM_PRICE, 100)))
                                .append("then", 1)
                                .append("else", 0)
                )
        ));
        Bson group = Aggregates.group(
                "$" + FIELD_SHOP_NAME,
                Accumulators.sum(countProducts, 1),
                Accumulators.max(maxPrice, "$" + alias + "." + FIELD_ITEM_PRICE),
                Accumulators.avg(avgPrice, "$" + alias + "." + FIELD_ITEM_PRICE),
                Accumulators.min(minPrice, "$" + alias + "." + FIELD_ITEM_PRICE),
                Accumulators.sum(priceLt100, "$" + fieldName)
        );

        AggregateIterable<Document> output = shops.aggregate(Arrays.asList(lookup, unwind, field, group));
        for (Document shop : output) {
            System.out.println("Магазин: " + shop.get("_id"));
            System.out.println("\tВсего товаров:        " + shop.get(countProducts));
            System.out.println("\tСамый дешёвый товар:  " + shop.get(minPrice));
            System.out.println("\tСамый дорогой товар:  " + shop.get(maxPrice));
            System.out.println("\tСредняя цена товара:  " + shop.get(avgPrice));
            System.out.println("\tТоваров, дешевле 100: " + shop.get(priceLt100));
            System.out.println();
        }
    }

    private static void clear(String key) {
        switch (key) {
            case "-s":
                clearShops();
                System.out.println("Список магазинов очищен.");
                break;
            case "-i":
                clearItems();
                System.out.println("Список товаров очищен.");
                break;
            default:
                System.out.println("Что очищаем?");
        }
    }

    public static void clearAll() {
        clearShops();
        clearItems();
        System.out.println("Списки магазинов и товаров очищены");
    }

    public static void clearShops() {
        shops.drop();
    }

    public static void clearItems() {
        items.drop();
    }

    public static void close() {
        mongoClient.close();
    }

    private Business() {
    }
}
