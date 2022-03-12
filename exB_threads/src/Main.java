import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;



public class Main implements Runnable {

    private int id;
    private ArrayList<Double> vector;

    public Main(int id, ArrayList<Double> vector){
        this.id = id;
        this.vector = vector;
    }
    @Override
    public void run() {
        FileReader fr = null;
        BufferedReader bf = null;
        try {
            fr = new FileReader("edgelist" + id + ".txt"  );
            bf = new BufferedReader(fr);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String line;
        int line_nb = id*vector.size()/ NOMBRE_DE_THREADS;
        try {
        while((line = bf.readLine()) !=null){
            int key = Integer.parseInt(line.split(" ")[0]) ;

            int val = (line.split(" ").length -1);

            String[] linearray = line.split(" ");

            double sum = 0.0;
            for(int i =0; i<vector.size(); ++i){
                sum += Double.parseDouble(linearray[i])*vector.get(i);

            }

            vector.set(line_nb, sum);

            line_nb++;
        }
        } catch (IOException e) {
            e.printStackTrace();
        }





    }


}

class Mainmain {
""
    public static void main(String[] args) throws InterruptedException {

        int size = 64375;
        ArrayList<Double> vector = new ArrayList<Double>();

        for(int i=0; i<size; i++){
            vector.add(1.0/size);
        }

        Thread[] threads = new Thread[4];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Main(i, vector));
        }




        while (true) {
            for (int i = 0; i < threads.length; i++) {
                threads[i].start();
            }

            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }

        }
    }

}