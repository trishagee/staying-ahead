package com.mechanitis.talks.stayingahead

import spock.lang.Specification

class ExampleSpecification extends Specification {
    def 'should be awesome'() {
        given:
        def example = new Example(database)
        
        when:
        example.saveOrder(order)
        
        then:
        order.id != null
    }

}
