import pika
import sqlite3
import json
import csv
import pandas as pd


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')

def callback (ch, method, properties, body):
    print("[X] Received %r" % body)
    data = json.loads(body)

    sq_link = data["link"]
    out_type = data["type"]

    con = sqlite3.connect(sq_link)
    c = con.cursor()

    # 1: for every song: genre and composer (if composer is null return the artist)
    c.execute("select distinct t.Name song_name, case when t.composer is null then ar.Name else t.Composer end composer_artist, g.Name genere_name from tracks t ,genres g , albums a, artists ar where t.GenreId = g.GenreId and t.AlbumId = a.AlbumId and a.ArtistId = ar.ArtistId order by 1")
    query1 = c.fetchall()

    # full customer list with full name and full address (if part of  the address is missing print none on the section) and how many purcheses made
    c.execute("select c.firstname ||' '|| c.lastname full_name, c.phone, c.email,  ifnull (c.address,'None') || '/'|| ifnull (c.city,'None') || '/' ||ifnull (c.state,'None') ||'/' || ifnull (c.country,'None') full_address, count(i.invoiceid) from customers c , invoices i where c.customerid = i.customerid group by c.firstname ||' '|| c.lastname , c.phone, c.email,  ifnull (c.address,'None') || '/'|| ifnull (c.city,'None') || '/' ||ifnull (c.state,'None') ||'/' || ifnull (c.country,'None') ")
    query2 = c.fetchall()

    # for every country which domains she has and how many customers on each domain
    c.execute("select country ,substr(substr(email, instr(email,'@')+1) ,1,instr(substr(email, instr(email,'@')+1),'.') - 1) domain , count(email) from customers group by country , substr(substr(email, instr(email,'@')+1) ,1,instr(substr(email, instr(email,'@')+1),'.') - 1)")
    query3 = c.fetchall()

    # for each country how many albums sold(after reviwing the data i concluded that customer will buy songs from the same album only in current invoice and wont buy it again, if 2 customers buy the album it will count as 2, if the same customer buy's it again it still count as 1(but i didnt see it happen))
    c.execute("select tb.country, sum(tb.num_sales_album) from (select c.country, t.albumid ,count(distinct c.customerid) num_sales_album from customers c, invoices i , invoice_items ii,tracks t where c.customerid = i.customerid and i.invoiceid=ii.invoiceid and ii.trackid = t.trackid group by c.country , t.albumid) tb group by tb.country")
    query4 = c.fetchall()

    # for each country return the albbum which most sold. if couple of albums sold the same, return the one with the most songs sold. wanted to do it with rank()over() but sqlite does not support it
    c.execute("with tb as (select c.country,a.title ,count(distinct c.customerid) num_sales_album , count(t.trackid) album_song_sold from customers c, invoices i , invoice_items ii,tracks t,albums a where c.customerid = i.customerid and i.invoiceid=ii.invoiceid and ii.trackid = t.trackid and t.albumid = a.albumid group by c.country , a.title) select country,title,num_sales_album,album_song_sold from tb where (country,tb.num_sales_album,album_song_sold) in (select country ,max(num_sales_album), max(album_song_sold) from tb group by country)")
    query5 = c.fetchall()

    # same as query 5 only for usa
    c.execute("with tb as (select c.country,a.title ,count(distinct c.customerid) num_sales_album , count(t.trackid) album_song_sold from customers c, invoices i , invoice_items ii,tracks t,albums a where c.customerid = i.customerid and i.invoiceid=ii.invoiceid and ii.trackid = t.trackid and t.albumid = a.albumid and c.country = 'USA' and i.invoicedate >= '2011-01-01' group by c.country , a.title) select country,title,num_sales_album,album_song_sold from tb where (country,tb.num_sales_album,album_song_sold) in (select country ,max(num_sales_album), max(album_song_sold) from tb group by country)")
    query6 = c.fetchall()

    # how many songs/series have the word black in their name
    c.execute("select count(*) num_times_black from tracks where name like '%Black%' ")
    query7 = c.fetchall()

    def one():
        with open('JSON Query1 Results', 'w') as json_file:
            json.dump(query1, json_file)

        with open('JSON Query2 Results', 'w') as json_file:
            json.dump(query2, json_file)

        with open('JSON Query3 Results', 'w') as json_file:
            json.dump(query3, json_file)

        with open('JSON Query4 Results', 'w') as json_file:
            json.dump(query4, json_file)

        with open('JSON Query5 Results', 'w') as json_file:
            json.dump(query5, json_file)

        with open('JSON Query6 Results', 'w') as json_file:
            json.dump(query6, json_file)

        with open('JSON Query7 Results', 'w') as json_file:
            json.dump(query7, json_file)

    def two():
        def row_to_xml(row):
            xml = ['<item>']
            for i, col_name in enumerate(row.index):
                xml.append('  <field name="{0}">{1}</field>'.format(col_name, row.iloc[i]))
            xml.append('</item>')
            return '\n'.join(xml)

        df = pd.DataFrame(query1)
        res = '\n'.join(df.apply(row_to_xml, axis=1))
        file = open('xml_result1.xml', 'w', encoding='utf8')
        file.write(res)

        df = pd.DataFrame(query2)
        res = '\n'.join(df.apply(row_to_xml, axis=1))
        file = open('xml_result2.xml', 'w', encoding='utf8')
        file.write(res)

        df = pd.DataFrame(query3)
        res = '\n'.join(df.apply(row_to_xml, axis=1))
        file = open('xml_result3.xml', 'w', encoding='utf8')
        file.write(res)

        df = pd.DataFrame(query4)
        res = '\n'.join(df.apply(row_to_xml, axis=1))
        file = open('xml_result4.xml', 'w', encoding='utf8')
        file.write(res)

        df = pd.DataFrame(query5)
        res = '\n'.join(df.apply(row_to_xml, axis=1))
        file = open('xml_result5.xml', 'w', encoding='utf8')
        file.write(res)

        df = pd.DataFrame(query6)
        res = '\n'.join(df.apply(row_to_xml, axis=1))
        file = open('xml_result6.xml', 'w', encoding='utf8')
        file.write(res)

        df = pd.DataFrame(query7)
        res = '\n'.join(df.apply(row_to_xml, axis=1))
        file = open('xml_result7.xml', 'w', encoding='utf8')
        file.write(res)

    def three():
        with open('csv_result1.csv', 'w', encoding='utf8', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(query1)
        csvfile.close()

        with open('csv_result2.csv', 'w', encoding='utf8', newline='') as c2:
            writer = csv.writer(c2)
            writer.writerows(query2)
        c2.close()

        with open('csv_result3.csv', 'w', encoding='utf8', newline='') as c3:
            writer = csv.writer(c3)
            writer.writerows(query3)
        c3.close()

        with open('csv_result4.csv', 'w', encoding='utf8', newline='') as c4:
            writer = csv.writer(c4)
            writer.writerows(query4)
        c4.close()

        with open('csv_result5.csv', 'w', encoding='utf8', newline='') as c5:
            writer = csv.writer(c5)
            writer.writerows(query5)
        c5.close()

        with open('csv_result6.csv', 'w', encoding='utf8', newline='') as c6:
            writer = csv.writer(c6)
            writer.writerows(query6)
        c6.close()

        with open('csv_result7.csv', 'w', encoding='utf8', newline='') as c7:
            writer = csv.writer(c7)
            writer.writerows(query7)
        c7.close()

    def four():
        c.execute("""CREATE TABLE RESULT_ONE AS
                   select distinct t.Name song_name, case when t.composer is null then ar.Name else t.Composer end composer_artist, g.Name genere_name from tracks t ,genres g , albums a, artists ar where t.GenreId = g.GenreId and t.AlbumId = a.AlbumId and a.ArtistId = ar.ArtistId order by 1 
                    ;""")
        c.execute("""CREATE TABLE RESULT_TWO AS
                    select c.firstname ||' '|| c.lastname full_name, c.phone, c.email,  ifnull (c.address,'None') || '/'|| ifnull (c.city,'None') || '/' ||ifnull (c.state,'None') ||'/' || ifnull (c.country,'None') full_address, count(i.invoiceid) from customers c , invoices i where c.customerid = i.customerid group by c.firstname ||' '|| c.lastname , c.phone, c.email,  ifnull (c.address,'None') || '/'|| ifnull (c.city,'None') || '/' ||ifnull (c.state,'None') ||'/' || ifnull (c.country,'None') 
                    ;""")
        c.execute("""CREATE TABLE RESULT_THREE AS
                    select country ,substr(substr(email, instr(email,'@')+1) ,1,instr(substr(email, instr(email,'@')+1),'.') - 1) domain , count(email) from customers group by country , substr(substr(email, instr(email,'@')+1) ,1,instr(substr(email, instr(email,'@')+1),'.') - 1)
                    ;""")
        c.execute("""CREATE TABLE RESULT_FOUR AS
                   select tb.country, sum(tb.num_sales_album) from (select c.country, t.albumid ,count(distinct c.customerid) num_sales_album from customers c, invoices i , invoice_items ii,tracks t where c.customerid = i.customerid and i.invoiceid=ii.invoiceid and ii.trackid = t.trackid group by c.country , t.albumid) tb group by tb.country 
                    ;""")
        c.execute("""CREATE TABLE RESULT_FIVE AS
                    with tb as (select c.country,a.title ,count(distinct c.customerid) num_sales_album , count(t.trackid) album_song_sold from customers c, invoices i , invoice_items ii,tracks t,albums a where c.customerid = i.customerid and i.invoiceid=ii.invoiceid and ii.trackid = t.trackid and t.albumid = a.albumid group by c.country , a.title) select country,title,num_sales_album,album_song_sold from tb where (country,tb.num_sales_album,album_song_sold) in (select country ,max(num_sales_album), max(album_song_sold) from tb group by country)
                    ;""")
        c.execute("""CREATE TABLE RESULT_SIX AS
                    with tb as (select c.country,a.title ,count(distinct c.customerid) num_sales_album , count(t.trackid) album_song_sold from customers c, invoices i , invoice_items ii,tracks t,albums a where c.customerid = i.customerid and i.invoiceid=ii.invoiceid and ii.trackid = t.trackid and t.albumid = a.albumid and c.country = 'USA' and i.invoicedate >= '2011-01-01' group by c.country , a.title) select country,title,num_sales_album,album_song_sold from tb where (country,tb.num_sales_album,album_song_sold) in (select country ,max(num_sales_album), max(album_song_sold) from tb group by country)
                    ;""")
        c.execute("""CREATE TABLE RESULT_SEVEN AS
                     select count(*) num_times_black from tracks where name like '%Black%'
                    ;""")

    switcher = {
        'JSON': one,
        'XML': two,
        'CSV': three,
        'TABLE': four
    }

    def export_by_type(argument):
        func = switcher.get(argument, "nothing")
        return func()

    export_by_type(out_type)

channel.basic_consume(callback,
                      queue='hello',
                      no_ack=True)

print('[*] Waiting for msg')
channel.start_consuming()



