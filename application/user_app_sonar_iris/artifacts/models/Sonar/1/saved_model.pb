д
о
:
Add
x"T
y"T
z"T"
Ttype:
2	

ApplyGradientDescent
var"T

alpha"T

delta"T
out"T" 
Ttype:
2	"
use_lockingbool( 

ArgMax

input"T
	dimension"Tidx
output"output_type" 
Ttype:
2	"
Tidxtype0:
2	"
output_typetype0	:
2	
x
Assign
ref"T

value"T

output_ref"T"	
Ttype"
validate_shapebool("
use_lockingbool(
R
BroadcastGradientArgs
s0"T
s1"T
r0"T
r1"T"
Ttype0:
2	
N
Cast	
x"SrcT	
y"DstT"
SrcTtype"
DstTtype"
Truncatebool( 
8
Const
output"dtype"
valuetensor"
dtypetype
S
DynamicStitch
indices*N
data"T*N
merged"T"
Nint(0"	
Ttype
B
Equal
x"T
y"T
z
"
Ttype:
2	

^
Fill
dims"
index_type

value"T
output"T"	
Ttype"

index_typetype0:
2	
?
FloorDiv
x"T
y"T
z"T"
Ttype:
2	
9
FloorMod
x"T
y"T
z"T"
Ttype:

2	
.
Identity

input"T
output"T"	
Ttype
,
Log
x"T
y"T"
Ttype:

2
p
MatMul
a"T
b"T
product"T"
transpose_abool( "
transpose_bbool( "
Ttype:
	2
;
Maximum
x"T
y"T
z"T"
Ttype:

2	

Mean

input"T
reduction_indices"Tidx
output"T"
	keep_dimsbool( " 
Ttype:
2	"
Tidxtype0:
2	
=
Mul
x"T
y"T
z"T"
Ttype:
2	
.
Neg
x"T
y"T"
Ttype:

2	

NoOp
C
Placeholder
output"dtype"
dtypetype"
shapeshape:

Prod

input"T
reduction_indices"Tidx
output"T"
	keep_dimsbool( " 
Ttype:
2	"
Tidxtype0:
2	
a
Range
start"Tidx
limit"Tidx
delta"Tidx
output"Tidx"
Tidxtype0:	
2	
>
RealDiv
x"T
y"T
z"T"
Ttype:
2	
5

Reciprocal
x"T
y"T"
Ttype:

2	
[
Reshape
tensor"T
shape"Tshape
output"T"	
Ttype"
Tshapetype0:
2	
o
	RestoreV2

prefix
tensor_names
shape_and_slices
tensors2dtypes"
dtypes
list(type)(0
l
SaveV2

prefix
tensor_names
shape_and_slices
tensors2dtypes"
dtypes
list(type)(0
P
Shape

input"T
output"out_type"	
Ttype"
out_typetype0:
2	
9
Softmax
logits"T
softmax"T"
Ttype:
2
:
Sub
x"T
y"T
z"T"
Ttype:
2	

Sum

input"T
reduction_indices"Tidx
output"T"
	keep_dimsbool( " 
Ttype:
2	"
Tidxtype0:
2	
c
Tile

input"T
	multiples"
Tmultiples
output"T"	
Ttype"

Tmultiplestype0:
2	
s

VariableV2
ref"dtype"
shapeshape"
dtypetype"
	containerstring "
shared_namestring "serve*1.12.02v1.12.0-0-ga6d8ffae09ў
d
xPlaceholder*
dtype0*'
_output_shapes
:џџџџџџџџџ<*
shape:џџџџџџџџџ<
e
y_Placeholder*
shape:џџџџџџџџџ*
dtype0*'
_output_shapes
:џџџџџџџџџ
~
W/Initializer/zerosConst*
_class

loc:@W*
valueB<*    *
dtype0*
_output_shapes

:<

W
VariableV2*
_output_shapes

:<*
shared_name *
_class

loc:@W*
	container *
shape
:<*
dtype0

W/AssignAssignWW/Initializer/zeros*
use_locking(*
T0*
_class

loc:@W*
validate_shape(*
_output_shapes

:<
T
W/readIdentityW*
_class

loc:@W*
_output_shapes

:<*
T0
v
b/Initializer/zerosConst*
_class

loc:@b*
valueB*    *
dtype0*
_output_shapes
:

b
VariableV2*
_class

loc:@b*
	container *
shape:*
dtype0*
_output_shapes
:*
shared_name 

b/AssignAssignbb/Initializer/zeros*
use_locking(*
T0*
_class

loc:@b*
validate_shape(*
_output_shapes
:
P
b/readIdentityb*
T0*
_class

loc:@b*
_output_shapes
:
R
zerosConst*
valueB*    *
dtype0*
_output_shapes
:
t
Accuracy
VariableV2*
dtype0*
_output_shapes
:*
	container *
shape:*
shared_name 

Accuracy/AssignAssignAccuracyzeros*
T0*
_class
loc:@Accuracy*
validate_shape(*
_output_shapes
:*
use_locking(
e
Accuracy/readIdentityAccuracy*
T0*
_class
loc:@Accuracy*
_output_shapes
:
4
initNoOp^Accuracy/Assign	^W/Assign	^b/Assign
s
MatMulMatMulxW/read*
T0*'
_output_shapes
:џџџџџџџџџ*
transpose_a( *
transpose_b( 
L
addAddMatMulb/read*
T0*'
_output_shapes
:џџџџџџџџџ
C
ySoftmaxadd*'
_output_shapes
:џџџџџџџџџ*
T0
?
LogLogy*
T0*'
_output_shapes
:џџџџџџџџџ
E
mulMuly_Log*'
_output_shapes
:џџџџџџџџџ*
T0
_
Sum/reduction_indicesConst*
valueB:*
dtype0*
_output_shapes
:
q
SumSummulSum/reduction_indices*#
_output_shapes
:џџџџџџџџџ*
	keep_dims( *

Tidx0*
T0
=
NegNegSum*
T0*#
_output_shapes
:џџџџџџџџџ
O
ConstConst*
valueB: *
dtype0*
_output_shapes
:
V
MeanMeanNegConst*
_output_shapes
: *
	keep_dims( *

Tidx0*
T0
R
gradients/ShapeConst*
valueB *
dtype0*
_output_shapes
: 
X
gradients/grad_ys_0Const*
dtype0*
_output_shapes
: *
valueB
 *  ?
o
gradients/FillFillgradients/Shapegradients/grad_ys_0*
T0*

index_type0*
_output_shapes
: 
k
!gradients/Mean_grad/Reshape/shapeConst*
valueB:*
dtype0*
_output_shapes
:

gradients/Mean_grad/ReshapeReshapegradients/Fill!gradients/Mean_grad/Reshape/shape*
_output_shapes
:*
T0*
Tshape0
\
gradients/Mean_grad/ShapeShapeNeg*
T0*
out_type0*
_output_shapes
:

gradients/Mean_grad/TileTilegradients/Mean_grad/Reshapegradients/Mean_grad/Shape*

Tmultiples0*
T0*#
_output_shapes
:џџџџџџџџџ
^
gradients/Mean_grad/Shape_1ShapeNeg*
out_type0*
_output_shapes
:*
T0
^
gradients/Mean_grad/Shape_2Const*
valueB *
dtype0*
_output_shapes
: 
c
gradients/Mean_grad/ConstConst*
valueB: *
dtype0*
_output_shapes
:

gradients/Mean_grad/ProdProdgradients/Mean_grad/Shape_1gradients/Mean_grad/Const*
_output_shapes
: *
	keep_dims( *

Tidx0*
T0
e
gradients/Mean_grad/Const_1Const*
valueB: *
dtype0*
_output_shapes
:

gradients/Mean_grad/Prod_1Prodgradients/Mean_grad/Shape_2gradients/Mean_grad/Const_1*
T0*
_output_shapes
: *
	keep_dims( *

Tidx0
_
gradients/Mean_grad/Maximum/yConst*
value	B :*
dtype0*
_output_shapes
: 

gradients/Mean_grad/MaximumMaximumgradients/Mean_grad/Prod_1gradients/Mean_grad/Maximum/y*
T0*
_output_shapes
: 

gradients/Mean_grad/floordivFloorDivgradients/Mean_grad/Prodgradients/Mean_grad/Maximum*
_output_shapes
: *
T0
~
gradients/Mean_grad/CastCastgradients/Mean_grad/floordiv*

SrcT0*
Truncate( *
_output_shapes
: *

DstT0

gradients/Mean_grad/truedivRealDivgradients/Mean_grad/Tilegradients/Mean_grad/Cast*
T0*#
_output_shapes
:џџџџџџџџџ
h
gradients/Neg_grad/NegNeggradients/Mean_grad/truediv*#
_output_shapes
:џџџџџџџџџ*
T0
[
gradients/Sum_grad/ShapeShapemul*
T0*
out_type0*
_output_shapes
:

gradients/Sum_grad/SizeConst*
_output_shapes
: *+
_class!
loc:@gradients/Sum_grad/Shape*
value	B :*
dtype0

gradients/Sum_grad/addAddSum/reduction_indicesgradients/Sum_grad/Size*
_output_shapes
:*
T0*+
_class!
loc:@gradients/Sum_grad/Shape
Ѕ
gradients/Sum_grad/modFloorModgradients/Sum_grad/addgradients/Sum_grad/Size*
_output_shapes
:*
T0*+
_class!
loc:@gradients/Sum_grad/Shape

gradients/Sum_grad/Shape_1Const*+
_class!
loc:@gradients/Sum_grad/Shape*
valueB:*
dtype0*
_output_shapes
:

gradients/Sum_grad/range/startConst*+
_class!
loc:@gradients/Sum_grad/Shape*
value	B : *
dtype0*
_output_shapes
: 

gradients/Sum_grad/range/deltaConst*+
_class!
loc:@gradients/Sum_grad/Shape*
value	B :*
dtype0*
_output_shapes
: 
Я
gradients/Sum_grad/rangeRangegradients/Sum_grad/range/startgradients/Sum_grad/Sizegradients/Sum_grad/range/delta*

Tidx0*+
_class!
loc:@gradients/Sum_grad/Shape*
_output_shapes
:

gradients/Sum_grad/Fill/valueConst*+
_class!
loc:@gradients/Sum_grad/Shape*
value	B :*
dtype0*
_output_shapes
: 
О
gradients/Sum_grad/FillFillgradients/Sum_grad/Shape_1gradients/Sum_grad/Fill/value*+
_class!
loc:@gradients/Sum_grad/Shape*

index_type0*
_output_shapes
:*
T0
ё
 gradients/Sum_grad/DynamicStitchDynamicStitchgradients/Sum_grad/rangegradients/Sum_grad/modgradients/Sum_grad/Shapegradients/Sum_grad/Fill*+
_class!
loc:@gradients/Sum_grad/Shape*
N*
_output_shapes
:*
T0

gradients/Sum_grad/Maximum/yConst*+
_class!
loc:@gradients/Sum_grad/Shape*
value	B :*
dtype0*
_output_shapes
: 
З
gradients/Sum_grad/MaximumMaximum gradients/Sum_grad/DynamicStitchgradients/Sum_grad/Maximum/y*
T0*+
_class!
loc:@gradients/Sum_grad/Shape*
_output_shapes
:
Џ
gradients/Sum_grad/floordivFloorDivgradients/Sum_grad/Shapegradients/Sum_grad/Maximum*
T0*+
_class!
loc:@gradients/Sum_grad/Shape*
_output_shapes
:
Ј
gradients/Sum_grad/ReshapeReshapegradients/Neg_grad/Neg gradients/Sum_grad/DynamicStitch*
T0*
Tshape0*0
_output_shapes
:џџџџџџџџџџџџџџџџџџ

gradients/Sum_grad/TileTilegradients/Sum_grad/Reshapegradients/Sum_grad/floordiv*'
_output_shapes
:џџџџџџџџџ*

Tmultiples0*
T0
Z
gradients/mul_grad/ShapeShapey_*
T0*
out_type0*
_output_shapes
:
]
gradients/mul_grad/Shape_1ShapeLog*
out_type0*
_output_shapes
:*
T0
Д
(gradients/mul_grad/BroadcastGradientArgsBroadcastGradientArgsgradients/mul_grad/Shapegradients/mul_grad/Shape_1*
T0*2
_output_shapes 
:џџџџџџџџџ:џџџџџџџџџ
m
gradients/mul_grad/MulMulgradients/Sum_grad/TileLog*
T0*'
_output_shapes
:џџџџџџџџџ

gradients/mul_grad/SumSumgradients/mul_grad/Mul(gradients/mul_grad/BroadcastGradientArgs*
	keep_dims( *

Tidx0*
T0*
_output_shapes
:

gradients/mul_grad/ReshapeReshapegradients/mul_grad/Sumgradients/mul_grad/Shape*
T0*
Tshape0*'
_output_shapes
:џџџџџџџџџ
n
gradients/mul_grad/Mul_1Muly_gradients/Sum_grad/Tile*
T0*'
_output_shapes
:џџџџџџџџџ
Ѕ
gradients/mul_grad/Sum_1Sumgradients/mul_grad/Mul_1*gradients/mul_grad/BroadcastGradientArgs:1*
	keep_dims( *

Tidx0*
T0*
_output_shapes
:

gradients/mul_grad/Reshape_1Reshapegradients/mul_grad/Sum_1gradients/mul_grad/Shape_1*
T0*
Tshape0*'
_output_shapes
:џџџџџџџџџ
g
#gradients/mul_grad/tuple/group_depsNoOp^gradients/mul_grad/Reshape^gradients/mul_grad/Reshape_1
к
+gradients/mul_grad/tuple/control_dependencyIdentitygradients/mul_grad/Reshape$^gradients/mul_grad/tuple/group_deps*
T0*-
_class#
!loc:@gradients/mul_grad/Reshape*'
_output_shapes
:џџџџџџџџџ
р
-gradients/mul_grad/tuple/control_dependency_1Identitygradients/mul_grad/Reshape_1$^gradients/mul_grad/tuple/group_deps*
T0*/
_class%
#!loc:@gradients/mul_grad/Reshape_1*'
_output_shapes
:џџџџџџџџџ

gradients/Log_grad/Reciprocal
Reciprocaly.^gradients/mul_grad/tuple/control_dependency_1*
T0*'
_output_shapes
:џџџџџџџџџ

gradients/Log_grad/mulMul-gradients/mul_grad/tuple/control_dependency_1gradients/Log_grad/Reciprocal*
T0*'
_output_shapes
:џџџџџџџџџ
h
gradients/y_grad/mulMulgradients/Log_grad/muly*
T0*'
_output_shapes
:џџџџџџџџџ
q
&gradients/y_grad/Sum/reduction_indicesConst*
valueB :
џџџџџџџџџ*
dtype0*
_output_shapes
: 
Ј
gradients/y_grad/SumSumgradients/y_grad/mul&gradients/y_grad/Sum/reduction_indices*
T0*'
_output_shapes
:џџџџџџџџџ*
	keep_dims(*

Tidx0
{
gradients/y_grad/subSubgradients/Log_grad/mulgradients/y_grad/Sum*
T0*'
_output_shapes
:џџџџџџџџџ
h
gradients/y_grad/mul_1Mulgradients/y_grad/suby*
T0*'
_output_shapes
:џџџџџџџџџ
^
gradients/add_grad/ShapeShapeMatMul*
T0*
out_type0*
_output_shapes
:
d
gradients/add_grad/Shape_1Const*
valueB:*
dtype0*
_output_shapes
:
Д
(gradients/add_grad/BroadcastGradientArgsBroadcastGradientArgsgradients/add_grad/Shapegradients/add_grad/Shape_1*
T0*2
_output_shapes 
:џџџџџџџџџ:џџџџџџџџџ

gradients/add_grad/SumSumgradients/y_grad/mul_1(gradients/add_grad/BroadcastGradientArgs*
	keep_dims( *

Tidx0*
T0*
_output_shapes
:

gradients/add_grad/ReshapeReshapegradients/add_grad/Sumgradients/add_grad/Shape*
T0*
Tshape0*'
_output_shapes
:џџџџџџџџџ
Ѓ
gradients/add_grad/Sum_1Sumgradients/y_grad/mul_1*gradients/add_grad/BroadcastGradientArgs:1*
T0*
_output_shapes
:*
	keep_dims( *

Tidx0

gradients/add_grad/Reshape_1Reshapegradients/add_grad/Sum_1gradients/add_grad/Shape_1*
T0*
Tshape0*
_output_shapes
:
g
#gradients/add_grad/tuple/group_depsNoOp^gradients/add_grad/Reshape^gradients/add_grad/Reshape_1
к
+gradients/add_grad/tuple/control_dependencyIdentitygradients/add_grad/Reshape$^gradients/add_grad/tuple/group_deps*
T0*-
_class#
!loc:@gradients/add_grad/Reshape*'
_output_shapes
:џџџџџџџџџ
г
-gradients/add_grad/tuple/control_dependency_1Identitygradients/add_grad/Reshape_1$^gradients/add_grad/tuple/group_deps*
T0*/
_class%
#!loc:@gradients/add_grad/Reshape_1*
_output_shapes
:
Г
gradients/MatMul_grad/MatMulMatMul+gradients/add_grad/tuple/control_dependencyW/read*
transpose_b(*
T0*'
_output_shapes
:џџџџџџџџџ<*
transpose_a( 
Ї
gradients/MatMul_grad/MatMul_1MatMulx+gradients/add_grad/tuple/control_dependency*
transpose_b( *
T0*
_output_shapes

:<*
transpose_a(
n
&gradients/MatMul_grad/tuple/group_depsNoOp^gradients/MatMul_grad/MatMul^gradients/MatMul_grad/MatMul_1
ф
.gradients/MatMul_grad/tuple/control_dependencyIdentitygradients/MatMul_grad/MatMul'^gradients/MatMul_grad/tuple/group_deps*
T0*/
_class%
#!loc:@gradients/MatMul_grad/MatMul*'
_output_shapes
:џџџџџџџџџ<
с
0gradients/MatMul_grad/tuple/control_dependency_1Identitygradients/MatMul_grad/MatMul_1'^gradients/MatMul_grad/tuple/group_deps*
T0*1
_class'
%#loc:@gradients/MatMul_grad/MatMul_1*
_output_shapes

:<
b
GradientDescent/learning_rateConst*
valueB
 *ЭЬЬ=*
dtype0*
_output_shapes
: 
ы
-GradientDescent/update_W/ApplyGradientDescentApplyGradientDescentWGradientDescent/learning_rate0gradients/MatMul_grad/tuple/control_dependency_1*
T0*
_class

loc:@W*
_output_shapes

:<*
use_locking( 
ф
-GradientDescent/update_b/ApplyGradientDescentApplyGradientDescentbGradientDescent/learning_rate-gradients/add_grad/tuple/control_dependency_1*
use_locking( *
T0*
_class

loc:@b*
_output_shapes
:
w
GradientDescentNoOp.^GradientDescent/update_W/ApplyGradientDescent.^GradientDescent/update_b/ApplyGradientDescent
P

save/ConstConst*
valueB Bmodel*
dtype0*
_output_shapes
: 
o
save/SaveV2/tensor_namesConst*#
valueBBAccuracyBWBb*
dtype0*
_output_shapes
:
i
save/SaveV2/shape_and_slicesConst*
valueBB B B *
dtype0*
_output_shapes
:
|
save/SaveV2SaveV2
save/Constsave/SaveV2/tensor_namessave/SaveV2/shape_and_slicesAccuracyWb*
dtypes
2
}
save/control_dependencyIdentity
save/Const^save/SaveV2*
T0*
_class
loc:@save/Const*
_output_shapes
: 

save/RestoreV2/tensor_namesConst"/device:CPU:0*#
valueBBAccuracyBWBb*
dtype0*
_output_shapes
:
{
save/RestoreV2/shape_and_slicesConst"/device:CPU:0*
valueBB B B *
dtype0*
_output_shapes
:
Љ
save/RestoreV2	RestoreV2
save/Constsave/RestoreV2/tensor_namessave/RestoreV2/shape_and_slices"/device:CPU:0* 
_output_shapes
:::*
dtypes
2

save/AssignAssignAccuracysave/RestoreV2*
use_locking(*
T0*
_class
loc:@Accuracy*
validate_shape(*
_output_shapes
:

save/Assign_1AssignWsave/RestoreV2:1*
T0*
_class

loc:@W*
validate_shape(*
_output_shapes

:<*
use_locking(

save/Assign_2Assignbsave/RestoreV2:2*
use_locking(*
T0*
_class

loc:@b*
validate_shape(*
_output_shapes
:
F
save/restore_allNoOp^save/Assign^save/Assign_1^save/Assign_2
Б
ArgMax/inputConst*ь
valueтBп*"аVSГ>UV&?5й	?Mь>кб?L\ш>хИю>Ѓ?'§>7l?SИд>жЃ?CК5?{>QЮ>Wѕ?џ?Ъу>{9ѓ>Bc?ъt*?+Ћ>еU?WTХ>Ј?БШл>ьЩ!?)lМ>ы?)Цк>Вмў>Ї ??ѓѕє>ќЮ	?bь>C	"?xэЛ>аХ?^tм>\8ш>бу?/Ь?Ѓgи>Пн> 6?яп?$@ќ>%ё?Зт>
Р*?юЊ>^%?DУД>Zќ>дИ?X]?NEС>5э>ve	?hИ№>ЬЃ?,	?гІэ>	?вЧэ>KЊ?kЋк>.Тм>щ?ZW?KQЩ>Pц?a3Ц>#?ќќИ>\"?GэК>Ѓ?Крр>У
?Шxъ>пђ>З?*
dtype0*
_output_shapes

:*
R
ArgMax/dimensionConst*
value	B :*
dtype0*
_output_shapes
: 
t
ArgMaxArgMaxArgMax/inputArgMax/dimension*

Tidx0*
T0*
output_type0	*
_output_shapes
:*

ArgMax_1/inputConst*М
valueВBЏ*"               №?      №?                      №?              №?      №?                      №?      №?                      №?              №?      №?              №?              №?              №?                      №?      №?                      №?              №?      №?              №?              №?                      №?      №?                      №?              №?      №?              №?              №?                      №?      №?                      №?      №?              №?                      №?      №?              №?                      №?              №?      №?              №?                      №?              №?              №?*
dtype0*
_output_shapes

:*
T
ArgMax_1/dimensionConst*
value	B :*
dtype0*
_output_shapes
: 
z
ArgMax_1ArgMaxArgMax_1/inputArgMax_1/dimension*

Tidx0*
T0*
output_type0	*
_output_shapes
:*
E
EqualEqualArgMaxArgMax_1*
T0	*
_output_shapes
:*
W
CastCastEqual*

SrcT0
*
Truncate( *
_output_shapes
:**

DstT0
Q
Const_1Const*
valueB: *
dtype0*
_output_shapes
:
[
Mean_1MeanCastConst_1*
T0*
_output_shapes
: *
	keep_dims( *

Tidx0
L
mul_1/yConst*
valueB
 *  ШB*
dtype0*
_output_shapes
: 
>
mul_1MulMean_1mul_1/y*
T0*
_output_shapes
: "D
save/Const:0save/control_dependency:0save/restore_all 5 @F8"О
trainable_variablesІЃ
2
W:0W/AssignW/read:02W/Initializer/zeros:08
2
b:0b/Assignb/read:02b/Initializer/zeros:08
9

Accuracy:0Accuracy/AssignAccuracy/read:02zeros:08"
train_op

GradientDescent"Д
	variablesІЃ
2
W:0W/AssignW/read:02W/Initializer/zeros:08
2
b:0b/Assignb/read:02b/Initializer/zeros:08
9

Accuracy:0Accuracy/AssignAccuracy/read:02zeros:08*g
model^

x
x:0џџџџџџџџџ<
y
y:0џџџџџџџџџtensorflow/serving/predict