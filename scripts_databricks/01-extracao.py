import requests
import random
from pyspark.sql.functions import lit
from bs4 import BeautifulSoup
from random import randrange
from datetime import timedelta,datetime


dbutils.fs.rm("dbfs:/databricks-results/ludo", True)


class Post:
  def __init__(self, topic_group, topic_name, user_name, post_time,url):
    self.topic_group = topic_group
    self.topic_name = topic_name
    self.user_name = user_name
    self.post_time = post_time
    self.url = url
  
  def __str__(self):
    return str(vars(self)).replace('{','(').replace('}',')')
  
  def __repr__(self):
    return str(self)


def random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def pegaPosts(URL):
    page = requests.get(URL)

    soup = BeautifulSoup(page.content, "html.parser")

    page_content = soup.find(id="page-content")
    order_list = page_content.find_all('ol')

    topic_group = order_list[0].find_all('li')[2].text
    topic_name = order_list[0].find_all('li')[3].text


    posts = page_content.find_all("div",class_ = 'post-top-bar')

    list_of_posts =  []

    for post in posts:
        user_name = post.find_all("div",class_ = 'pull-left')[1].find_all("a",class_ = 'post-top-bar-user-xs')[0].text
        post_time = post.find_all("div",class_ = 'pull-left')[1].find_all("small",class_ = 'text-muted block')[0].text
        list_of_posts.append(Post(topic_group,topic_name,user_name,post_time,URL))

    return list_of_posts
    
   
def salvar_arquivo_parquet(list_of_posts,topic_index,page_index):
    path = f"dbfs:/databricks-results/ludo/bronze/t{topic_index}/p{page_index}"
    response = list_of_posts
    df_converted = spark.createDataFrame(response)

    df_converted.write.format("parquet")\
                        .mode("overwrite")\
                        .save(path)

    print(f"Os arquivos foram salvos em {path}")




def create_mock(dataframe):

    d1 = datetime.strptime('1/1/2013 1:30 PM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('11/12/2023 7:56 PM', '%m/%d/%Y %I:%M %p')

    array_of_topic_group = []

    for row in dataframe.select('topic_group').distinct().collect():
        array_of_topic_group.append(row['topic_group'])

    array_of_topic_name = []

    for row in dataframe.select('topic_name').distinct().collect():
        array_of_topic_name.append(row['topic_name'])

    array_of_user_name = []

    for row in dataframe.select('user_name').distinct().collect():
        array_of_user_name.append(row['user_name'])

    list_of_posts =  []

    for i in range(1,500001):
        print(f"Add mock person id:{i}")
        random_topic_group = random.randint(0, len(array_of_topic_group)-1)
        random_topic_name = random.randint(0, len(array_of_topic_name)-1)
        random_user_name = random.randint(0, len(array_of_user_name)-1)
        random_time = random_date(d1, d2).strftime("%d/%m/%Y %H:%M:%S")

        list_of_posts.append(Post(array_of_topic_group[random_topic_group], \
                                array_of_topic_name[random_topic_name], \
                                array_of_user_name[random_user_name], \
                                random_time, \
                                ''))
        
    return list_of_posts



END = 500001

COUNT = 0 
DOC_END = 0

for i in range(1,END):
    for j in range(1,51):
        try:
            URL = f"https://ludopedia.com.br/topico/{i}?pagina={j}"   
            print(URL)
            list_of_posts = pegaPosts(URL)
            COUNT = COUNT + len(list_of_posts)
            salvar_arquivo_parquet(list_of_posts,i,j)
        except:
            break
    print(COUNT)
    if COUNT > 2000:
        DOC_END = i + 1
        break

mock_data = create_mock(spark.read.parquet("dbfs:/databricks-results/ludo/bronze/*/*"))

salvar_arquivo_parquet(mock_data,DOC_END,1)