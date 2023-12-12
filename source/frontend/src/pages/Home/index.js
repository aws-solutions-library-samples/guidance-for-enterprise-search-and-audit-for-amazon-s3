import React from "react";
import { AutoComplete, TextField } from 'rsuite';
import Results from "components/Results"

const Home = (props) => {

    //const search_terms = ['apple', 'apple watch', 'apple macbook', 'apple macbook pro', 'iphone', 'iphone 12'];
    let search_terms = []

    const url = process.env.REACT_APP_API_GATEWAY_URL + 'autocomplete?q='
    const [searchTerm, setSearchTerm] = React.useState('')
    const [searchTerms, setSearchTerms] = React.useState([])

    React.useEffect(() => {
        async function fetchData() {
            const res = await fetch(url + searchTerm);
            const data = await res.json();
            for (let match in data) {
                search_terms.push(data[match]['search_field'].toLowerCase())
            }

            setSearchTerms(search_terms)
        }
        
        fetchData().catch(console.error);
    }, [searchTerm]);

    // function autocompleteMatch(input) {
    //     if (input == '') {
    //         return [];
    //         //setSearchTerms('')
    //     }

    //     const reg = new RegExp(input)
    //     return search_terms.filter(function(term) {
    //         if (term.match(reg)) {
    //             return term;
    //         }
    //     }); 
    // }

    // function showResults(val) {
    //     let res = document.getElementById("result");
    //     res.innerHTML = '';
    //     let list = '';
    //     let terms = autocompleteMatch(searchTerm);
    //     for (let i=0; i<terms.length; i++) {
    //         list += '<li>' + terms[i] + '</li>';
    //     }
    //     res.innerHTML = '<ul>' + list + '</ul>';
    //     res.style.display = "block"
    // }
    
    function handleChange(value, event) {
        //console.log(event.target.value)       
        //const search_string = event.target.value
        setSearchTerm(value);
    }

    function handleSubmit(item, event) {
        //event.preventDefault();
        console.log(item)

        const search_string = item;
        //console.log(search_string)
        props.setSearchState({
            'query': {
                'sort': 'object_name',
                'sort_dir': 'asc',
                't': 'search',
                'q': search_string,
                'bucket': '',
                'path': '',
                'per_page': 50,
                'page': 1,
                'filters': {}
            }
        });

        setSearchTerms([])

        //document.getElementById("result").style.display = 'none';
    }

    let welcome_screen
    if (!('items' in props.searchResults)) {
        welcome_screen =
        <div className="mt-5">
            <h3>Welcome to S3 Search and Audit</h3>
            <p>
                Select any account and bucket on the left to see details or search below
            </p>
        </div>
    }

    return (
        <div className="col text-center">
            {welcome_screen}

            {/* <AutoComplete
                    id="auto_complete"
                    data={searchTerms}
                    onChange={handleChange}
                    onSelect={handleSubmit}
                    style={{ width: "100%" }}
                    placeholder="Search"
                    onKeyUp={e => {if (e.key === 'Enter') {handleSubmit(document.getElementById('auto_complete').value, e)}}}
                /> */}

            <AutoComplete
                    id="auto_complete"
                    data={searchTerms}
                    style={{ width: "100%" }}
                    placeholder="Search"
                    onKeyUp={e => {if (e.key === 'Enter') {handleSubmit(document.getElementById('auto_complete').value, e)}}}
                />

            <Results searchState={props.searchState} setSearchState={props.setSearchState} searchResults={props.searchResults} key="1" />
        </div>
    );
};

export default Home;
