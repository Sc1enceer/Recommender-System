package entity;

public class Item {

    private int itemId;

    public Item(int itemId) {
        this.itemId = itemId;
    }

    public int getItemId() {
        return itemId;
    }

    @Override
    public String toString() {
        return "item [itemId=" + itemId + "]";
    }
}