import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class EigenVectorCentrality {

    public static void main(String[] args) throws InterruptedException {

        final int size = 64375;
        final int nb_threads = 5;
        ArrayList<Double> vector = new ArrayList<Double>();

        for(int i = 0; i < size; i++){
            vector.add(1.0/size);
        }

        Thread[] threads = new Thread[nb_threads];

        int converge = 0;
        while (converge < 1000) {
            for(int i = 0; i < nb_threads; i++) {
                threads[i] = new Thread(new EigenVectorCentralityThread(i, vector));
                threads[i].start();
            }
            for(int i = 0; i < nb_threads; i++) {
                threads[i].join();
            }
            double norm = 0;
            for(double i : vector) {
                norm+=i*i;
            }
            norm = Math.sqrt(norm);
            for(int i = 0; i < vector.size(); i++) {
                vector.set(i, vector.get(i)/norm);
            }
            converge++;
        }

        //vector.forEach(s -> System.out.println(s));

        System.out.println(Collections.max(vector));
        System.out.println(vector.indexOf(Collections.max(vector)));
    }
}