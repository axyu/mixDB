#include <iostream>

#include "supersonic/supersonic.h"

using std::cout;

using std::endl;

using supersonic::BoundExpressionTree;

using supersonic::Expression;

using supersonic::Plus;

using supersonic::AttributeAt;

using supersonic::TupleSchema;

using supersonic::Attribute;

using supersonic::INT32;

using supersonic::NOT_NULLABLE;

using supersonic::FailureOrOwned;

using supersonic::HeapBufferAllocator;

using supersonic::View;

using supersonic::EvaluationResult;

using supersonic::SingleSourceProjector;




BoundExpressionTree* PrepareBoundexpression_r() {

        scoped_ptr<const Expression> addition(Plus(AttributeAt(0), AttributeAt(1)));

        TupleSchema schema;

    schema.add_attribute(Attribute("a", INT32, NOT_NULLABLE));

        schema.add_attribute(Attribute("b", INT32, NOT_NULLABLE));

        FailureOrOwned<BoundExpressionTree> bound_addition =

              addition->Bind(schema, HeapBufferAllocator::Get(), 2048);




        if(bound_addition.is_success()) {

                return bound_addition.release();

        }




        return NULL;

}




const int32* AddColumns(int32* a, int32* b, size_t row_count, BoundExpressionTree* bound_tree) {

        TupleSchema schema;

        schema.add_attribute(Attribute("a", INT32, NOT_NULLABLE));

        schema.add_attribute(Attribute("b", INT32, NOT_NULLABLE));

        View input_view(schema);

        input_view.set_row_count(row_count);

        input_view.mutable_column(0)->Reset(a, NULL);

        input_view.mutable_column(1)->Reset(b, NULL);

        EvaluationResult result = bound_tree->Evaluate(input_view);

        if(result.is_success()) {

                cout << "Column Count : " << result.get().column_count() << 

                        " and Row Count" << result.get().row_count() << endl;

                return result.get().column(0).typed_data<INT32>();

        }




        return NULL;

}




int main(void) {

        int32 a[8] = {0, 1, 2, 3,  4, 5, 6,  7};

        int32 b[8] = {3, 4, 6, 8,  1, 2, 2,  9};




        scoped_ptr<BoundExpressionTree> expr(PrepareBoundexpression_r());

        const int32* result = AddColumns(a, b, 8, expr.get());




        if(result == NULL) {

                cout << "Failed to execute the addition operation!" << endl;

        }




        cout << "Results: ";

        for(int i = 0; i < 8; i++) {

                cout << result[i] << " ";

        }




        return 0;

}
