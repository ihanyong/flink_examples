package com.monkey.flink.state.examples;

import java.io.Serializable;

/**
 * ColorShape
 *
 * @author yong.han
 * 2019/5/8
 */
public class ColorShape implements Serializable {


    private Color color;
    private Shape shape;

    public ColorShape() {
    }

    public ColorShape(Color color, Shape shape) {
        this.color = color;
        this.shape = shape;
    }

    public enum Color {
        green, red, yellow, grey
    }
    public enum Shape {
        triangle, rectangle, pentagon, trapezoid, circle
    }

    public static ColorShape of(String text, String delimiter) {
        if (null == text || text.length() == 0) {
            throw new IllegalArgumentException("cannot construct ColorShape from empty string");
        }

        if (delimiter == null || delimiter.length() == 0) {
            delimiter = ",";
        }

        return of(text.split(delimiter));
    }

    public static ColorShape of(String[] strs) {
        if (strs == null || strs.length != 2) {
            throw new IllegalArgumentException("ColorShape need and only need tow args");
        }
        return new ColorShape(Color.valueOf(strs[0]), Shape.valueOf(strs[1]));
    }




    public Color getColor() {
        return color;
    }

    public void setColor(Color color) {
        this.color = color;
    }

    public Shape getShape() {
        return shape;
    }

    public void setShape(Shape shape) {
        this.shape = shape;
    }

    @Override
    public String toString() {
        return "ColorShape{" +
                "color=" + color +
                ", shape=" + shape +
                '}';
    }
}
