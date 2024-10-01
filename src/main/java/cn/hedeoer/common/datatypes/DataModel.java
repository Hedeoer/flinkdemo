package cn.hedeoer.common.datatypes;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public  class DataModel{
    private String SourceSubtaskNumber;
    private String TransSubTaskName;

    @Override
    public String toString() {
        return SourceSubtaskNumber +" ===> "+ TransSubTaskName;
    }
}