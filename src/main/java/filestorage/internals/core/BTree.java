package filestorage.internals.core;

import java.lang.reflect.Array;

public class BTree<K extends Comparable<K>, V> {
    private final static int LIMIT = 4;

    private Node root;
    private int height;
    private int size;

    public BTree() {
        root = new Node(0);
    }

    public V get(K key) {
        return search(root, key, height);
    }

    public void add(K key, V value) {
        Node right = insert(root, key, value, height);
        size++;
        if (right == null) {
            return;
        }

        // need to split root
        Node new_root = new Node(2);
        new_root.children[0] = new Entry(root.children[0].key, null, root);
        new_root.children[1] = new Entry(right.children[0].key, null, right);
        root = new_root;
        height++;
    }

    public String toString() {
        return toString(root, height, "");
    }

    private V search(Node node, K key, int height) {

        // external node, Leaf Node
        if (height == 0) {

            for (int i = 0; i < node.filled; i++) {
                if (eq(node.children[i].key, key) || more(key, node.children[i].key)) {
                    return node.children[i].value;
                }
            }
        }
        // internal node
        else {
            for (int i = 0; i < node.filled; i++) {
                if (i + 1 == node.filled || less(key, node.children[i + 1].key)) {
                    return search(node.children[i].next, key, height - 1);
                }
            }
        }

        return null;
    }

    private Node insert(Node node, K key, V value, int height) {
        Entry entry = new Entry(key, value, null);

        //  leaf Node
        if (height == 0) {

            int position = node.filled;

            for (int i = node.filled - 1; i >= 0; i--, position--) {
                if (less(key, node.children[i].key)) {
                    node.children[i + 1] = node.children[i];
                }
                else {
                    break;
                }
            }

            node.children[position] = entry;
            node.filled++;
        }
        // non leaf node
        else {

            int position = node.filled;

            for (int i = node.filled - 1; i >= 0; i--, position--) {

                if (more(key, node.children[i].key)) {
                    Node insert = insert(node.children[i].next, key, value, height - 1);
                    if (insert == null) {
                        return null;
                    }

                    entry.key = insert.children[0].key;
                    entry.value = null;
                    entry.next = insert;
                    break;
                }
            }

            for (int i = node.filled - 1; i > position; i--) {
                node.children[i + 1] = node.children[i];
            }

            node.children[position] = entry;
            node.filled++;
        }
        return node.filled < LIMIT ? null : split(node);
    }

    private Node split(Node root) {
        Node node = new Node(LIMIT / 2);
        root.filled = LIMIT / 2;

        for (int i = 0; i < LIMIT / 2; i++) {
            node.children[i] = root.children[LIMIT / 2 + i];
        }
        return node;
    }

    private boolean more(K a, K b) {
        return a.compareTo(b) > 0;
    }

    private boolean eq(K a, K b) {
        return a.compareTo(b) == 0;
    }

    private boolean less(K a, K b) {
        return a.compareTo(b) < 0;
    }

    private String toString(Node node, int height, String indent) {
        StringBuilder s = new StringBuilder();
        Entry[] children = node.children;

        if (height == 0) {
            for (int j = 0; j < node.filled; j++) {
                s.append(indent + children[j].key + " " + children[j].value + "\n");
            }
        }
        else {
            for (int j = 0; j < node.filled; j++) {
                if (j > 0) {
                    s.append(indent + "(" + children[j].key + ")\n");
                }
                s.append(toString(children[j].next, height - 1, indent + "     "));
            }
        }
        return s.toString();
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int size() {
        return size;
    }

    private class Node {
        private int filled;
        private Entry[] children = (Entry[]) Array.newInstance(Entry.class, LIMIT); // Entry[] children = new Entry[LIMIT] won't compile, because it scares java

        public Node(int initially) {
            this.filled = initially;
        }
    }

    private class Entry {
        K key;
        V value;
        Node next;

        public Entry(K key, V value, Node next) {
            this.key = key;
            this.value = value;
            this.next = next;
        }
    }
}
