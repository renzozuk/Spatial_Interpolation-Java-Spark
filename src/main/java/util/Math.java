package util;

public class Math {
    public static final double DEGREES_TO_RADIANS = 0.017453292519943295;

    public static double pow(double x, int y) {
        if (y < 0) {
            return 1 / pow(x, -y);
        }

        double result = 1.0;

        for (int i = 0; i < y; i++) {
            result *= x;
        }

        return result;
    }
}
