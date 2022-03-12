import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SharedVector {

    private ArrayList<Double> vector;
    private Lock lock;

    public SharedVector() {
        vector = new ArrayList<Double>();
        lock = new ReentrantLock();
    }

    public void add(double value) {
        lock.lock();
        vector.add(value);
        lock.unlock();
    }

    public void set(int index, double value) {
        lock.lock();
        vector.set(index, value);
        lock.unlock();
    }

    public double get(int index) {
        lock.lock();
        double v = vector.get(index);
        lock.unlock();
        return v;
    }

    public int size() {
        lock.lock();
        int s = vector.size();
        lock.unlock();
        return s;
    }
}