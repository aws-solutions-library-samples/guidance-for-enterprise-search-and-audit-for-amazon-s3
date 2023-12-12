import React, { useState } from "react";
import Navbar from "pages/_layouts/Home/Navbar"
import Home from "pages/Home";

const Filebrowser = () => {

    const [searchState, setSearchState] = React.useState({
        'query': {
            'sort': 'object_name',
            'sort_dir': 'asc',
            't': '',
            'q': '',
            'bucket': '',
            'path': '',
            'per_page': 50,
            'page': 1,
            'filters': {}
        }
    })

    //console.log(searchState)

    const [searchResults, setSearchResults] = React.useState({})

    React.useEffect(() => {
        async function fetchData() {
            //console.log(searchState)

            const search_url = process.env.REACT_APP_API_GATEWAY_URL + 'search?'
            let url = ''
            if (searchState['query']['bucket'].length) {
                url = '&t=path&b=' + encodeURIComponent(searchState['query']['bucket'].trim())
                if (searchState['query']['path'].length) {
                    url += '&q=' + encodeURIComponent(searchState['query']['path'])
                }
            }

            if (searchState['query']['q'].length) {
                url = '&t=search&q=' + encodeURIComponent(searchState['query']['q']);
            }

            //console.log(url)
            const hasFilters = Object.keys(searchState['query']['filters']).length !== 0;

            let sort = (searchState['query']['sort'].length) ? searchState['query']['sort'] : 'object_name';
            let sort_dir = (searchState['query']['sort_dir'].length) ? searchState['query']['sort_dir'] : 'asc';

            if (url.length) {
                url = 's=' + sort + '&sd=' + sort_dir + '&p=' + searchState['query']['page'] + '&pp=' + searchState['query']['per_page'] + url
                if (hasFilters) {
                    if (searchState['query']['filters'].hasOwnProperty('object_name') && searchState['query']['filters']['object_name'].length) url = url + '&f_object_name=' + searchState['query']['filters']['object_name']
                    if (searchState['query']['filters'].hasOwnProperty('size_from') && searchState['query']['filters']['size_from'].length) url = url + '&f_size_from=' + searchState['query']['filters']['size_from']
                    if (searchState['query']['filters'].hasOwnProperty('size_to') && searchState['query']['filters']['size_to'].length) url = url + '&f_size_to=' + searchState['query']['filters']['size_to']
                    if (searchState['query']['filters'].hasOwnProperty('last_write') && searchState['query']['filters']['last_write'].length) url = url + '&f_write=' + searchState['query']['filters']['last_write']
                    if (searchState['query']['filters'].hasOwnProperty('last_read') && searchState['query']['filters']['last_read'].length) url = url + '&f_read=' + searchState['query']['filters']['last_read']
                }
                url = search_url + url
                const res = await fetch(url);
                const data = await res.json();
                setSearchResults(data);
            }
        }
        
        fetchData().catch(console.error);
    }, [searchState]);

    return (
        <div id="content">
            <div className="row">
                <Navbar searchState={searchState} setSearchState={setSearchState} />
                <Home searchState={searchState} setSearchState={setSearchState} searchResults={searchResults} />
            </div>
        </div>
    )

}

export default Filebrowser;