import re

from app.__init__ import *
from app.utils import generate_thumb_link
from mediawiki.wiki import create_wiki_page, create_redirect_wiki_page

CR = "\n"
BR = "<br/>" + CR
LIST_ITEM = "* {} : {}\n"
ALEF_LINK = "http://aleph.nli.org.il/F?func=direct&local_base={}&doc_number={}"
WIKI_LINK = "[[{}|{}]]"  # first the link then the display text
AUTHORITY_ID_PATTERN = "\$\$D(.*)\$\$E(.*)\$\$I(.*)\$\$P"  # e.g. "$$Dרכטר, יוני, 1951-$$Eרכטר, יוני, 1951-$$INNL10000110663$$PY M"
# view online - is the template for placing the thumbnail and view online links in an item page
VIEW_ONLINE = '{| class="wikitable" style="margin-left:0px;margin-right:auto"' + CR + \
'| width="120" height="120" style="vertical-align: middle; text-align: center" | [http://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_pid={IE} <img src="http://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_pid={IE}&dps_func=thumbnail" style="max-height:150px; max-width: 150px"/>]' + CR + \
'|-' + CR + \
'|{title}' + CR + \
'|}' + CR


month_num_to_heb_name = {
    '01': 'ינואר',
    '02': 'פברואר',
    '03': 'מרץ',
    '04': 'אפריל',
    '05': 'מאי',
    '06': 'יוני',
    '07': 'יולי',
    '08': 'אוגוסט',
    '09': 'ספטמבר',
    '10': 'אוקטובר',
    '11': 'נובמבר',
    '12': 'דצמבר',
}


def date8_to_heb_date(date8):
    """
    convert 8 digit date to date in hebrew
    :param date8: 8 digits date YYYYMMDD, e.g. 18861006, or 6 digit YYYYMM or 4 digits YYYY
    :return: date in Hebrew, e.g. ״6 באוקטבר 1886״
    """
    date8 = date8.replace("-","")
    date8 = date8.replace("~", "")
    if len(date8) == 0:
        return ""
    year = date8[0:4]
    if len(date8) <= 4:
        return year
    month = month_num_to_heb_name[date8[4:6]]
    if len(date8) == 6:
        return "{} {}".format(month, year)
    day = str(int(date8[6:8]))
    return "{} ב{} {}".format(day, month, year)


def str_to_list(str_or_list):
    if str_to_list is None:
        return None
    if type(str_or_list) is str:
        return [str_or_list]
    else:
        return str_or_list


def comma_and(line):
    """
    in a comma-separated list of terms, replace the last comma with וו החיבור.
    :param line: one or more comma-separated terms
    :return: last comma (if exists) replaced with "ו"
    """
    return ' ו'.join(line.rsplit(', ', maxsplit=1))


def entries_to_authority_id(browse_entries):
    """
    Create a dictionary from authority terms to their NNL record id. The input is a list of
    lines like this: "$$Dרכטר, יוני, 1951-$$Eרכטר, יוני, 1951-$$INNL10000110663$$PY M". Here the
    key is "רכטר, יוני, 1951-" and the value is "000110663"
    :param browse_entries: list of authority entries
    :return: a dictionary from authority name to NNL record id
    """
    authority_dictionary = {}
    for author in browse_entries:
        match = re.search(AUTHORITY_ID_PATTERN, author)
        if match:
            authority_dictionary[match.group(2)] = match.group(3)[5:]
    return authority_dictionary


def simple_person_name(primo_person_name):
    if primo_person_name[-1] == ",":
        primo_person_name = primo_person_name[:-1]
    splitted = primo_person_name.split(", ", 2)
    if len(splitted) == 1:
        return splitted[0]
    return splitted[1] + " " + splitted[0]


def person_name(persons_to_id, primo_person_name):
    """
    Convert "last, first, year" to "first last" (year is optional)
    :param name: person name as "last, first, other"
    :return: first last
    """
    wiki_person_name = primo_person_name
    link = persons_to_id.get(primo_person_name)
    if not link:
        person_name_no_role = primo_person_name[:primo_person_name.rfind(" ")]
        link = persons_to_id.get(person_name_no_role)
        if link:
            wiki_person_name = person_name_no_role
    display_name = simple_person_name(wiki_person_name)
    if link:
        return WIKI_LINK.format(link, display_name)
    else:
        return display_name


def is_hebrew(line):
    return re.match(".*[א-ת].*", line) is not None


def trim(line):
    """
    Trim non-alpha characters from the end of the line. Leave parentheses, quotes.
    For example trime("abc def..") returns "abc def"
    :param line: a string
    :return: the same string without trailing non-alpha characters
    """
    clean_line = line
    while clean_line and not clean_line[-1].isalnum() and not clean_line[-1] in '")':
        clean_line = clean_line[:-1]
    return clean_line


def handle_categories(browse, create_category_pages):
    """
    :param browse: list of names of categories with the category ID in the DB
    :param create_category_pages: whether the algorithm should create category pages when encountered
    :return:
    """
    subjects = {}
    if browse.get('subject'):
        subjects = str_to_list(browse['subject'])
    nnl_to_subject = {}
    for subject in subjects:
        match = re.search(AUTHORITY_ID_PATTERN, subject)
        if match:
            nnl = match.group(3)[5:]
            term = match.group(1)
            if is_hebrew(term):  # Hebrew term
                nnl_to_subject[nnl] = term

    result = ""
    for subject in nnl_to_subject.values():
        category_page_name = "קטגוריה:{}".format(subject)
        result += CR + '[[{}]]'.format(category_page_name)
        if create_category_pages:
            create_wiki_page(category_page_name, "Creating empty category page", "")

    return result


BAD = "<>[]{}"


def clean_title(title):
    for bad in BAD:
        title = title.replace(bad, " ")
    return title


def stablize(barr, length):
    barr = barr[:length]
    try:
        sarr = barr.decode()
    except:
        return stablize(barr, length - 1)
    return sarr


def limit_length(title):
    b_title = bytes(title, encoding='utf8')
    if len(b_title) <= 255:
        return title
    return stablize(b_title, 252) + '...'


def create_page_from_dictionary(item_dict, debug=None, create_category_pages=False, site=None):
    """
    create a wikipedia page from a dictionary that describes a primo item
    :param item_dict: primo item as a dictionary/json
    :param debug: if not debug then actually create the pages
    :param create_category_pages: whether to create empty category pages when encountered
    :return: page content in wiki markup
    """
    document_id = item_dict['control']['recordid']
    sourcerecordid = item_dict['control']['sourcerecordid']
    originalsourceid = item_dict['control']['originalsourceid']
    display = item_dict['display']
    try:
        title = display['title']
        if type(title) is list:
            title = title(0) # getting the main title short, the full title is displayed in another location in the page
    except:
        title = trim(item_dict['sort']['title'])
    item_type = display['type'].lower()

    try:
        display_type = type_dict[item_type][1]  # hebrew type as a definite article, e.g. כתב העת
        display_type += " "
    except Exception as e:
        display_type = ""
        print("Unrecognized type '{}'".format(item_type))

    creation_verb = type_dict[item_type][2]
    creators_field = display.get('creator')
    if display.get('contributor'):
        if creators_field:
            creators_field += creators_field + ';' + display.get('contributor')
        else:
            creators_field = display.get('contributor')

    creator = None
    if creators_field:
        authors_to_id = entries_to_authority_id(str_to_list(item_dict['browse']['author']))
        creators = creators_field.split(";")
        creator = ", ".join(set([person_name(authors_to_id, creator.strip()) for creator in creators]))
        creator = comma_and(creator)
    else:
        authors_to_id = {}

    summary = display.get('lds20')

    comments = str_to_list(display.get('lds05'))
    comments_section = None
    if comments:
        comments_section = CR.join(["* " + comment for comment in comments])

    # handle digital images: thumbnail display + links to digital images
    rosetta_link = item_dict["links"].get("linktorsrc")
    view_online = ''
    if rosetta_link:
        # handling the 'view' button & thumbnail image
        thumb_value = generate_thumb_link(rosetta_link)
        if thumb_value:
            view_online = VIEW_ONLINE
            view_online = view_online.replace('{IE}',thumb_value[0])
            view_online = view_online.replace('{title}', title)
    creationdate = display.get('creationdate')
    ispartof = display.get('ispartof')
    performed_by = display.get('lds35')  # list
    performed_by = str_to_list(performed_by)

    performed_by_str = None
    if performed_by:
        performed_by_str = ", ".join(person_name(authors_to_id, performer) for performer in performed_by)

    source = display['source']
    lib_link = display.get('lds21')
    if not lib_link:
        lib_link = item_dict['links']['linktorsrc']
        lib_link = lib_link[lib_link.find("http"):]

    # Building page's Wikicode
    content = "{{DISPLAYTITLE:%s}}\n" % title
    content += "{}'''{}''' {}".format(display_type, title, creation_verb)
    if creator:
        content += " על ידי {}".format(creator)

    if (creationdate):
        content += " בשנת {}".format(creationdate)
    content += CR
    if summary:
        content += CR + summary + CR

    if view_online:
        if len(view_online)>0:
            content += view_online + CR
    content += "==פרטים כלליים==" + CR
    if (performed_by_str):
        content += LIST_ITEM.format("שם מבצע", performed_by_str)
    if (ispartof):
        content += LIST_ITEM.format("מתוך", ispartof)
    if comments_section:
        content += comments_section
    content += CR + "==מידע נוסף==" + CR
    content += LIST_ITEM.format("מקור", source)
    content += "* מספר מערכת: [{} {}]\n".format(lib_link, sourcerecordid)
    content += "== קישורים נוספים ==\n"
    alef_link = ALEF_LINK.format(originalsourceid, sourcerecordid)
    content += "* [{} הפריט בקטלוג הספרייה הלאומית]\n".format(alef_link)

    browse = item_dict.get('browse')
    if browse:
        content += handle_categories(item_dict['browse'], create_category_pages)

    if debug:
        print(content)
    else:
        title = clean_title(title)
        if is_hebrew(title):
            title = limit_length(title)
            create_redirect_wiki_page(site, page_name=clean_title(title), redirect_to=document_id,
                                      summary="Creating redirect page for {}".format(document_id))
        create_wiki_page(site, page_name=document_id, summary="Created from primo", content=content)

    return content
