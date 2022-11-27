package common;

/**
 * @author WtMonster
 * @date 2022/11/27 20:38
 */
public class TestCommon {
    public static void main(String[] args) throws Exception {
        Object aReturn = getReturn();
        System.out.println(aReturn);
    }

    private static Object getReturn() throws Exception {
        throw new Exception();
    }


}
