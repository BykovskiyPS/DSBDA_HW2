package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.math.BigInteger;

@Data
@AllArgsConstructor
public class Element implements Serializable {
    // Значение элемента массива
    public BigInteger value;

    // Индекс элемента массива
    public Integer index;
}
