from app.pages import CR, BR, simple_person_name, date8_to_heb_date
from app.wiki import create_wiki_page, create_redirect_wiki_page
import json

with open("templates/personality.wiki.template") as f:
    TEMPLATE = f.read()

template_name = "שם="
template_image_url = "תמונה="
template_birth_date = "תאריך לידה="
template_death_date = "תאריך פטירה="
template_birth_place = "מקום לידה="
template_death_place = "מקום פטירה="
template_address = "מקום מגורים="
template_other_names = "כינויים נוספים="

def personality_name(record):
    return record['person_name_heb']

def item(record, tag, code):
    pass

def get_if_exists(dict, *keys):
    dict0 = dict
    try:
        for key in keys:
            dict0 = dict0[key]
        return dict0
    except:
        return ""

def create_page_from_node(person_node, debug=None, create_category_pages=False):
    """
    Create a person page from a neo4j node
    :param person_node: neo4j node
    :param debug:
    :param create_category_pages:
    :return:
    """
    wiki_page_name = person_node['id']
    person_name = simple_person_name(person_node['person_name_heb'])
    record = json.loads(person_node['data'])

    birth_date = date8_to_heb_date(get_if_exists(record, '046', 0, 'f'))
    death_date = date8_to_heb_date(get_if_exists(record, '046', 0, 'g'))

    birth_place = get_if_exists(record, '370', 0, 'a')
    death_place = get_if_exists(record, '370', 0, 'b')

    other_names = get_if_exists(record, '400')
    other_names_value = BR.join(simple_person_name(other_name['a']) for other_name in other_names)

    address = get_if_exists(record, '371', 0, 'a')
    address_place = get_if_exists(record, '371', 0, 'b')
    address_country = get_if_exists(record, '371', 0, 'd')

    occupation = get_if_exists(record, '374', 0, 'a')
    gender = get_if_exists(record, '375', 'a') # MALE/FEMALE

    value_image_url = ""

    template = TEMPLATE \
        .replace(template_name, template_name + person_name) \
        .replace(template_birth_date, template_birth_date + birth_date) \
        .replace(template_death_date, template_death_date + death_date) \
        .replace(template_birth_place, template_birth_place + birth_place) \
        .replace(template_death_place, template_death_place + death_place) \
        .replace(template_other_names, template_other_names + other_names_value) \

    content = "{{DISPLAYTITLE:%s}}\n" % person_name

    content += template

    notes1 = record.get('678')
    notes2 = record.get('680')
    notes3 = record.get('681')

    notes = []
    for notes_i in (notes1, notes2, notes3):
        if notes_i:
            notes += notes_i

    if notes:
        content += CR + "== הערות ==" + CR
        content += "".join(note['a'] + BR for note in notes)

    if debug:
        print(content)
    else:
        redicrect_page_name = "אישיות:" + person_name
        # create_redirect_wiki_page(page_name=redicrect_page_name, redirect_to=wiki_page_name,
        #                           summary="Creating redirect page for {}".format(wiki_page_name))
        create_wiki_page(page_name=wiki_page_name, summary="Created from primo", content=content)

    return content