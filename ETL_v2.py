import pandas as pd

#Leitura dos arquivos CSV em DataFrames
pizzas = pd.read_csv('pizzas.csv')
orders = pd.read_csv('orders.csv')
pizza_types = pd.read_csv('pizza_types.csv',encoding= 'unicode_escape')
order_details = pd.read_csv('order_details.csv')

#União dos dados de pedidos e dos detalhes dos pedidos
orders_complete = order_details.merge(
    orders, left_on='order_id',right_on='order_id',how='left'
    )

#União com os dados dos tipos de pizza, tamanhos, preços e ingredientes
orders_complete = orders_complete.merge(
    pizzas, left_on='pizza_id',right_on='pizza_id',how='left'
    )
orders_complete = orders_complete.merge(
    pizza_types, left_on='pizza_type_id',right_on='pizza_type_id',how='left'
    )

#Separação e contagem dos ingredientes por tipo de pizza
ingredients = pizza_types[['pizza_type_id','ingredients']]
ingredients_list = ingredients['ingredients'].drop_duplicates().tolist()

ingredients_list2 = []

for i in ingredients_list:
    x = i.split(',')
    for _ in x:
        ingredients_list2.append(_.strip())
        
ingredients_list = list(set(ingredients_list2))

for _ in ingredients_list:
    ingredients[f'{_}'] = 0
    
def set_ingredients (row, var):
    response = 0
    if var in row:
        response = 1
    else:
        response = 0
    return response

for _ in ingredients_list:
    ingredients[f'{_}'] = ingredients['ingredients'].apply(
        lambda x: set_ingredients(x, _)
        )
    
type_count = orders_complete['pizza_type_id'].value_counts().to_frame().reset_index()
type_count = type_count.rename(
    columns={'index':'pizza_type_id','pizza_type_id':'value_count'}
    )
ingredients = ingredients.merge(type_count,
                                left_on='pizza_type_id',
                                right_on='pizza_type_id',
                                suffixes=('_ingredients','_count'),
                                how='left'
                                )
ingredients[ingredients_list] = ingredients[ingredients_list].multiply(
    ingredients['value_count'], axis="index"
    )

#Criação de um DF somente com os ingredientes e suas quantidades totais
d = {}
for _ in ingredients_list:
    x = ingredients[f'{_}'].sum()
    d.update(
        {f'{_}':x}
        )

d_index = list(d.keys())
d_values = list(d.values())
d = {
     'ingredients':d_index,
     'count':d_values}
ingredients_count = pd.DataFrame(data=d)

#Formatação das horas para exportação para o Tableau
orders_complete['time'] = orders_complete['date']+' '+orders_complete['time']
#Cálculo do preço total
orders_complete['total_price'] = orders_complete['price']*orders_complete['quantity']

#Salva os arquivos CSV
ingredients_count.to_csv('ingredients_count.csv',index=False)
orders_complete.to_csv('orders_complete.csv',index=False)

orders_complete = pd.read_csv('orders_complete.csv')