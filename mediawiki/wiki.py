import mwclient

def get_wiki_page(site, page_name):
    page = site.Pages[page_name]
    return page

def create_wiki_page(site, page_name, summary, content):
    page = get_wiki_page(page_name=page_name, site=site)
    page.save(content, summary)
    return page

def create_redirect_wiki_page(site, page_name, redirect_to, summary):
    page = get_wiki_page(page_name=page_name, site=site)
    content = "#REDIRECT [[{}]]".format(redirect_to)
    page.save(content, "Creating redirect from {} to {}".format(page_name, redirect_to))
    return page

def delete_wiki_page(site, page_name, reason=None):
    page = get_wiki_page(page_name=page_name, site=site)
    page.delete(reason=reason)
