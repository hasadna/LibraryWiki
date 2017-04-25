import os
import sys

sys.path.append(os.path.join(os.getcwd(), '..'))

from mediawiki.pages import CR, BR, simple_person_name, date8_to_heb_date
from mediawiki.wiki import create_wiki_page
from app.utils import generate_thumb_link, extract_link
from app.__init__ import *
import json


with open("templates/personality.wiki.template", encoding="utf8") as f:
    TEMPLATE = f.read()

COLLAPSIBLE = 'class="mw-collapsible mw-collapsed wikitable" width=100%'
LINE_BREAK = '|-' + CR
OPENDIV = '<div style="width: 750px;">'
CLOSEDIV = '</div>'

VIEW_ONLINE = '([{} לצפיה])'
GALLERY_ITEM = '''| width="200" |[http://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_pid={0}&dps_func=stream <img src="http://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_pid={0}&dps_func=thumbnail" style="max-height:150px; max-width: 150px"/>]'''
ITEM = '{{|class="mw-collapsible mw-collapsed wikitable" width=100%' + CR + \
       '!{view}&nbsp; {title} &nbsp;' + CR + \
       LINE_BREAK + \
       '{thumb}' + \
       '| הפריט המלא: [[{nnl_prefix}{nnl}|{description}]]' + CR + \
       LINE_BREAK + \
       '|{notes}' + CR + \
       LINE_BREAK + \
       '|תאריך : {date}' + CR + \
       LINE_BREAK + \
       '|[http://primo.nli.org.il/primo_library/libweb/action/dlDisplay.do?vid=NLI&docId={nnl_prefix}{nnl} הרשומה באתר הספרייה הלאומית ({nnl})]' + CR + \
       '|}}' + CR

VIEW_TEMPLATE = '''|<img src="http://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_pid={}&dps_func=thumbnail"/>
|-
'''

template_name = "שם="
template_image_url = "תמונה="
template_birth_date = "תאריך לידה="
template_death_date = "תאריך פטירה="
template_birth_place = "מקום לידה="
template_death_place = "מקום פטירה="
template_address = "מקום מגורים="
template_other_names = "כינויים נוספים="
template_occupation = "מקצוע="


def personality_name(record):
    return record['person_name_heb']


def item(record, tag, code):
    pass


def get_if_exists(diction, *keys):
    dict0 = diction
    try:
        for key in keys:
            dict0 = dict0[key]
        return dict0
    except:
        return ""


def generate_thumb(rosetta_links):
    """
    This function returns a thumbnail link in the format needed for personality pages
    :param rosetta_links: that contains IE PID
    :return: The part of a wiki-table to be used for displaying thumbnail
    """
    ies = generate_thumb_link(rosetta_links)
    if ies:
        return VIEW_TEMPLATE.format(ies[0])
    return ''


def create_page_from_node(person_node, records_list, debug=None, create_category_pages=False, site=None):
    """
    Create a person page from a neo4j node
    :param person_node: neo4j node
    :param debug:
    :param create_category_pages:
    :return:
    """
    wiki_page_name = person_node['id']
    person_name = simple_person_name(person_node['person_name_heb'])
    this_record = json.loads(person_node['data'])

    birth_date = date8_to_heb_date(get_if_exists(this_record, '046', 0, 'f'))
    death_date = date8_to_heb_date(get_if_exists(this_record, '046', 0, 'g'))

    birth_place = get_if_exists(this_record, '370', 0, 'a')
    death_place = get_if_exists(this_record, '370', 0, 'b')

    other_names = get_if_exists(this_record, '400')
    other_names_value = BR.join(simple_person_name(other_name['a']) for other_name in other_names)

    address = get_if_exists(this_record, '371', 0, 'a')
    address_place = get_if_exists(this_record, '371', 0, 'b')
    address_country = get_if_exists(this_record, '371', 0, 'd')

    occupation = get_if_exists(this_record, '374', 0, 'a')
    gender = get_if_exists(this_record, '375', 0, 'a')  # MALE/FEMALE
    female = gender.lower() == "female"

    value_image_url = ""

    content = "{{DISPLAYTITLE:%s}}\n" % person_name

    AUDIO = ["==פריטי שמע==", OPENDIV]
    VIDEO = ["==פריטי וידאו==", OPENDIV]
    BOOKS_BY = ["==ספרים שכתבה==" if female else "==ספרים שכתב==", OPENDIV]
    BOOKS_ABOUT = ["==ספרים אודותיה==" if female else "==ספרים אודותיו==", OPENDIV]
    IMAGES = ["==גלריית תמונות==", '{| class="wikitable" border="1"']
    IMAGES_DESC = ['|-']
    OTHER = ["==אחר==", OPENDIV]

    for record_rel in records_list:
        for record_type in records_list[record_rel]:
            for record in records_list[record_rel][record_type]:
                # getting record type
                if record_type == "other":
                    item_type = "other"
                else:
                    item_type = type_dict.get(record_type.lower())[3]

                # temporary - skip over non-hebrew records
                if item_type == 'print':
                    if record['language']:
                        if record['language']!='heb':
                            continue
                # content_item = ITEM.format(**record)
                content_item = ITEM
                rosetta_link = record['rosetta'] or ''
                if len(rosetta_link) > 0:
                    # handling the 'view' button & thumbnail image
                    view_online = VIEW_ONLINE
                    view_online = view_online.format(extract_link(rosetta_link))
                    content_item = content_item.replace('{view}', view_online)
                    thumb_value = generate_thumb(rosetta_link)
                    if thumb_value:
                        content_item = content_item.replace('{thumb}',thumb_value)
                    else:
                        content_item = content_item.replace('{thumb}','')
                else:
                    content_item = content_item.replace('{view}', '')
                    content_item = content_item.replace('{thumb}','')
                content_item = content_item.format(**record)
                if record_rel == 'portrait_of':
                    value_image_url = '<img src="http://rosetta.nli.org.il/delivery/DeliveryManagerServlet?' \
                                      'dps_pid={}&dps_func=stream"' \
                                      ' style="max-height: 500px; max-width: 300px;"/>'.format(record['fl'])

                if item_type == 'print':
                    if record_rel == 'subject_of':
                        BOOKS_ABOUT.append(content_item)
                    else:
                        BOOKS_BY.append(content_item)
                elif item_type == 'audio':
                    AUDIO.append(content_item)
                elif item_type == 'video':
                    VIDEO.append(content_item)
                elif item_type == 'photograph':
                    if len(IMAGES_DESC) > 6 or not record.get('fl'):
                        continue
                    IMAGES.append(GALLERY_ITEM.format(record['fl']))
                    IMAGES_DESC.append('| {}'.format(record['title']))
                else:
                    OTHER.append(content_item)

    template = TEMPLATE \
        .replace(template_name, template_name + person_name) \
        .replace(template_birth_date, template_birth_date + birth_date) \
        .replace(template_death_date, template_death_date + death_date) \
        .replace(template_birth_place, template_birth_place + birth_place) \
        .replace(template_death_place, template_death_place + death_place) \
        .replace(template_other_names, template_other_names + other_names_value) \
        .replace(template_occupation, template_occupation + occupation) \
        .replace(template_image_url, template_image_url + value_image_url)

    content += template

    notes1 = this_record.get('670')
    notes2 = this_record.get('678')
    notes3 = this_record.get('680')

    notes = []
    for notes_i in (notes1, notes2, notes3):
        if notes_i:
            notes += notes_i

    if notes:
        content += CR
        content += "".join(note['a'] + BR for note in notes if note.get('a') and note.get('a') != "LCN")

    IMAGES_DESC.append('|}')

    content += CR + \
               CR.join(BOOKS_BY) + CLOSEDIV + CR + \
               CR.join(BOOKS_ABOUT) + CLOSEDIV + CR + \
               CR.join(AUDIO) + CLOSEDIV + CR + \
               CR.join(VIDEO) + CLOSEDIV + CR + \
               CR.join(IMAGES + IMAGES_DESC) + CR + \
               CR.join(OTHER) + CLOSEDIV + CR

    if debug:
        print(content)
    else:
        redicrect_page_name = "אישיות:" + person_name
        # create_redirect_wiki_page(page_name=redicrect_page_name, redirect_to=wiki_page_name,
        #                           summary="Creating redirect page for {}".format(wiki_page_name))
        create_wiki_page(site, page_name=wiki_page_name, summary="Created from primo", content=content)

    return content
