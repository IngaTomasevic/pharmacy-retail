from dataset_create import call_gpt
from dataset_create import generate_products_hierarchy
from dataset_create import join_data_frames_one_unique
from dataset_create import subsequent_days
from dataset_create import subsequent_times
import pandas as pd
from faker import Faker
from datetime import date
import random
import re

if __name__ == '__main__':
    print('HI INga')
    """ To use 'dataset_create' module you need to pay for the OpenAi usage.
     The minimum payment is enough (~3 $) for multiple calls that could be extended if needed
     when generating and testing datasets"""

    # adjust the request based on area you need, provided example is for pharmaceutical products sales set
    req_categories = ('Generate 5 medication categories names. Generate concise medication categories names /'
                      'without any additional description and without such words as "medication(s)" or "drug". /'
                      'Provide the result as enumerated list of values each starting with a new line. /'
                      'Please do not provide empty values.')

    req_states = ('Provide 3 USA states names. Provide only concise states names without any additional description /'
                  'and without such word as "state". Provide the result as enumerated list of values each starting /'
                  'with a new line. Please do not provide empty values.')

    req_customers = ('Provide 100 english full names. Provide only concise full names without any additional /'
                     'description. Provide the result as enumerated list of values each starting /'
                     'with a new line. Please do not provide empty values.')

    req_brands = ('Provide 50 brand names for pharmaceutical products. Provide only concise brand names without any /' 
                  'additional description. Provide the result as enumerated list of values each starting /'
                  'with a new line. Please do not provide empty values.')

    req_suppliers = ('Provide 50 suppliers names for pharmaceutical products. Provide only concise suppliers names /' 
                     'without any additional description. Provide the result as enumerated list of values each /'
                     'starting with a new line. Please do not provide empty values.')

    path_write_categories = f'D:\project_materials\datasets2025\medication_class.csv'
    path_write_products = f'D:\project_materials\datasets2025\products.csv'
    path_write_addresses = f'D:\project_materials\datasets2025\\addresses.csv'
    path_write_customers = f'D:\project_materials\datasets2025\customers.csv'
    path_write_brands = f'D:\project_materials\datasets2025\brands.csv'
    path_write_suppliers = f'D:\project_materials\datasets2025\suppliers.csv'
    path_write_dates = f'D:\project_materials\datasets2025\dates.csv'
    path_write_times = f'D:\project_materials\datasets2025\\times.csv'
    path_write_sales_events = f'D:\project_materials\datasets2025\\sales_events.csv'
    path_write_sales_events_final = f'D:\project_materials\datasets2025\\sales_events_final.csv'

    """
    Generate Brands Dimension, export into csv *************************************************************************
    """
    brands = call_gpt(req_brands)
    brands_list = [w.strip('0123456789. "') for w in brands.split('\n')]
    brand_id_list = [x for x in range(1, len(brands_list) + 1)]

    dd = {'brand_id': brand_id_list,
          'brand_name': brands_list
          }

    df_brands = pd.DataFrame(dd)
    # df_brands.reset_index(drop=True, inplace=True)
    # df_brands.to_csv(path_write_brands, index=False)

    """
    Generate products hierarchy Dimension, export into csv *************************************************************
    """
    category = call_gpt(req_categories)
    ctg_list = [w.strip('0123456789. "') for w in category.split('\n')]

    dfs_list = []
    for cat in ctg_list:
        cat_id = ctg_list.index(cat) + 1
        sub_ctg_list = generate_products_hierarchy('category', 'subcategories names',
                                                   cat, 1, 3)
        for sub_ctg in sub_ctg_list:
            form_list = []
            price_list = []
            products_list = generate_products_hierarchy('subcategory', 'product names',
                                                        cat, 3, 6)

            for prod in products_list:
                form = generate_products_hierarchy('product', 'most popular form',
                                                   prod, 1, 1)[0]

                req_price = (f'Provide one single price value for {prod} based on statistic of prices for {prod} /'
                             f'in USA for 2 last years. Provide only float positive value with precision of 2 /'
                             f'decimal places without currency symbol, without "$" sign and without any text.')
                price_str = call_gpt(req_price)
                price_float = float(re.sub(r'[$a-zA-Z]', '', price_str))

                form_list.append(form)
                price_list.append(price_float)

            m = len(products_list)

            dd = {'category_id': [cat_id]*m,
                  'category': [cat]*m,
                  'subcategory': [sub_ctg]*m,
                  'product': products_list[0:len(products_list)],
                  'form': form_list[0:len(form_list)],
                  'price': price_list[0:len(price_list)]}

            df = pd.DataFrame(dd)
            dfs_list.append(df)

    df_products = pd.concat(dfs_list)
    df_products['product_id'] = [x + 1 for x in range(len(df_products))]
    df_products.reset_index(drop=True, inplace=True)
    df_products = join_data_frames_one_unique(df_products, df_brands, 1, 1)
    df_products.reset_index(drop=True, inplace=True)
    # df_products.to_csv(path_write_products, index=False)

    """
    Generate addresses hierarchy Dimension, export into csv ************************************************************
    """
    states = call_gpt(req_states)
    states_list = [w.strip('0123456789. "') for w in states.split('\n')]

    dfs_list = []
    city_id = 1
    for state in states_list:
        state_id = states_list.index(state) + 1
        cities_list = generate_products_hierarchy('state', 'cities names', state,
                                                  10, 20)

        for city in cities_list:
            city_id += 1
            streets_list = generate_products_hierarchy('city', 'streets names', city,
                                                       5, 18)

            for street in streets_list:
                req_zip = (f'Provide one zip code value for the address: {state}, {city}, {street}. /'
                           f'Provide the result as exact zip code value without any additional text.')
                zip_str = call_gpt(req_zip)
                build_list = [random.randint(1, 260) for n in range(random.randint(1, 70))]

                m = len(build_list)

                dd = {'state_id': [state_id]*m,
                      'state': [state]*m,
                      'city_id': [city_id]*m,
                      'city': [city]*m,
                      'street_name': [street]*m,
                      'zip_code': [zip_str]*m,
                      'build_numb': build_list[0:len(build_list)]}

                df = pd.DataFrame(dd)
                dfs_list.append(df)

    df_addresses = pd.concat(dfs_list)
    df_addresses['address_id'] = [x + 1 for x in range(len(df_addresses))]
    df_addresses.reset_index(drop=True, inplace=True)
    # df_addresses.to_csv(path_write_addresses, index=False)

    """
    Generate Customers Dimension, export into csv **********************************************************************
    """
    customers = call_gpt(req_customers)
    customers_list = [w.strip('0123456789. "') for w in customers.split('\n')]
    fake = Faker()
    bd_start = date(1956, 1, 1)
    bd_end = date(2004, 7, 1)
    r_start = date(2017, 1, 1)
    r_end = date(2021, 12, 31)

    cust_birthdate_list = [fake.date_between(bd_start, bd_end) for x in range(len(customers_list))]
    user_registration_list = [fake.date_between(r_start, r_end) for x in range(len(customers_list))]
    customer_email_list = [fake.email().replace('example', 'gmail') for i in range(len(customers_list))]
    customer_phone_list = [fake.phone_number() for i in range(len(customers_list))]
    customer_id_list = [x for x in range(1, len(customers_list)+1)]

    dd = {'customer_id': customer_id_list,
          'customer_full_name': customers_list,
          'cust_birthdate': cust_birthdate_list,
          'user_registration': user_registration_list,
          'customer_email': customer_email_list,
          'customer_phone': customer_phone_list
          }

    df_customers = pd.DataFrame(dd)
    df_customers = join_data_frames_one_unique(df_customers, df_addresses, 1, 1)
    df_customers.reset_index(drop=True, inplace=True)
    # df_customers.to_csv(path_write_customers, index=False)

    """
    Generate Suppliers Dimension, export into csv **********************************************************************
    """
    suppliers = call_gpt(req_suppliers)
    suppliers_list = [w.strip('0123456789. "') for w in suppliers.split('\n')]
    supplier_id_list = [x for x in range(1, len(suppliers_list) + 1)]

    dd = {'supplier_id': supplier_id_list,
          'supplier_name': suppliers_list
          }

    df_suppliers = pd.DataFrame(dd)
    df_suppliers.reset_index(drop=True, inplace=True)
    # df_suppliers.to_csv(path_write_suppliers, index=False)

    """
    Generate Dates Dimension, export into csv **************************************************************************
    """
    df_dates = subsequent_days(2023, 2023)
    df_dates.reset_index(drop=True, inplace=True)
    # df_dates.to_csv(path_write_dates, index=True)

    """
    Generate Times Dimension, export into csv **************************************************************************
    """
    df_times = subsequent_times(0, 14)
    df_times.reset_index(drop=True, inplace=True)
    # df_times.to_csv(path_write_times, index=True)

    """
    Generate Sales events, export into csv *****************************************************************************
    """
    df_sale_events = join_data_frames_one_unique(df_dates, df_customers, 1, 10)
    df_sale_events.reset_index(drop=True, inplace=True)
    df_sale_events_with_time = join_data_frames_one_unique(df_sale_events, df_times, 1, 1)
    df_sale_events_with_time.reset_index(drop=True, inplace=True)
    # df_sale_events_with_time.to_csv(path_write_sales_events, index=True)
    df_sale_events_prod = join_data_frames_one_unique(df_sale_events_with_time, df_products, 1, 3)
    df_sale_events_prod['quantity'] = [random.randint(1, 3) for x in range(len(df_sale_events_prod))]
    df_sale_events_prod.reset_index(drop=True, inplace=True)
    df_sale_events_prod_suppl = join_data_frames_one_unique(df_sale_events_prod, df_suppliers, 1, 1)
    df_sale_events_prod_suppl.reset_index(drop=True, inplace=True)
    df_sale_events_prod_suppl.to_csv(path_write_sales_events_final, index=False)
