# -*- coding: utf-8 -*-
import redis
from lxml import etree, objectify

pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
db = redis.Redis(connection_pool=pool)


def load_xml_to_redis(file, data_type):
    tree = etree.parse(file)
    root = tree.getroot()

    for elem in root.getiterator():
        if not hasattr(elem.tag, 'find'): continue  # (1)
        i = elem.tag.find('}')
        if i >= 0:
            elem.tag = elem.tag[i + 1:]
    objectify.deannotate(root, cleanup_namespaces=True)

    for elem in root:
        for sub_elems in elem:

            id = str(sub_elems.attrib)
            id = id.replace("b\'{{http://www.opengis.net/gml/3.2}id: ", "")
            id = id.replace("}\"", "")
            id = id.replace("\'", "")
            id = id.replace("}", "")
            id = id.replace("{{http://www.opengis.net/gml/3.2id: ", "")

            db.hset(id, 'Typ_danych', data_type)

            for attributes in sub_elems:
                if attributes.text is not None:
                    if len(attributes.text.split()) != 0:
                        db.hset(id, attributes.tag, str(attributes.text))

                    else:
                        for sub_attribute in attributes:
                            if sub_attribute in attributes:
                                if len(sub_attribute.text.split()) != 0:
                                    db.hset(id, attributes.tag + "_" +sub_attribute.tag, str(sub_attribute.text))

                                else:
                                    for sub_attribute_level_2 in sub_attribute:
                                        if sub_attribute_level_2 in sub_attribute:
                                            if len(sub_attribute_level_2.text.split()) != 0:
                                                db.hset(id, attributes.tag + "_" + sub_attribute_level_2.tag,
                                                        str(sub_attribute_level_2.text))

                                            else:
                                                for sub_attribute_level_3 in sub_attribute_level_2:
                                                    if sub_attribute_level_3 in sub_attribute_level_2:
                                                        if len(sub_attribute_level_3.text.split()) != 0:
                                                            db.hset(id,
                                                                    attributes.tag + "_" + sub_attribute_level_3.tag,
                                                                    str(sub_attribute_level_3.text))

                                                        else:
                                                            for sub_attribute_level_4 in sub_attribute_level_3:
                                                                if sub_attribute_level_4 in sub_attribute_level_3:
                                                                    if len(sub_attribute_level_4.text.split()) != 0:
                                                                        db.hset(id,
                                                                                attributes.tag + "_" + sub_attribute_level_4.tag,
                                                                                str(sub_attribute_level_4.text))


def reading_database(data_type):
    keys = db.keys()

    for key in keys:

        if data_type == 'ADMS_P':
            if str(db.hget(key, 'Typ_danych')) == 'b\'ADMS_P\'':
                print(db.hgetall(key))

        if data_type== 'BUBD_A':
            if str(db.hget(key, 'Typ_danych')) == 'b\'BUBD_A\'':
                print(db.hgetall(key))

        if data_type == 'OIKM_P':
            if str(db.hget(key, 'Typ_danych')) == 'b\'OIKM_P\'':

                print(db.hgetall(key))



def main():

    file = "PL.PZGiK.994.0463__OT_ADMS_P.xml"
    data_type = "ADMS_P"



    #load_xml_to_redis(file, data_type)

    reading_database(data_type)


    #Deleting  all entries
    #db.flushall()


main()





