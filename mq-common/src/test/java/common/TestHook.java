package common;

/**
 * @author WtMonster
 * @date 2022/11/30 1:38
 */
public class TestHook {
    public static void main(String[] args) throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            public void run()
            {
                System.out.println("Shutdown Hook now!");
            }
        });
        Thread.sleep(100000);
        System.out.println("Going to exit");
    }
}
