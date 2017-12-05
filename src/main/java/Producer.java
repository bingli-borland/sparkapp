import java.util.Random;

public class Producer {

    public static void main(String[] args) {
        final Random random = new Random();
        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    
                    String sex = String.valueOf(random.nextInt(2));
                    KafkaUtils.sendMsgToKafka(sex);
                }
            }
        });
        thread.start();
    }
    
}
