import React from "react";
import "./index.css";

const Paging = (props) => {

    const total_pages = Math.ceil(props.totalCount/50)
    const current_page = props.searchState.query.page

    let page_links = [], start = 0, end = 0, dots = ''

    function pageClick(go_to_page) {
        if (go_to_page >= 1 && go_to_page <= total_pages) {
            props.setSearchState(prevState => {
                let query = Object.assign({}, prevState.query)
                query.page = go_to_page
                return { query };  
            })
        }
    }

    if (total_pages <= 7) {
        start = 1;
        end = total_pages
    } else if (current_page - 3 <= 1) {
        start = 1
        end = 4
        dots = 'end'
    } else if (current_page + 5 >= total_pages) {
        start = total_pages - 5
        end = total_pages
        dots = 'begin'
    } else {
        start = current_page - 2
        end = current_page + 2
        dots = 'both'
    }

    if (dots === 'begin' || dots === 'both') {
        page_links.push(
            <button className="pagination-number" onClick={() => {pageClick(1)}}>1</button>
        )
        page_links.push(
            <span>...</span>
        )
    }

    for (let i = start; i <= end; i++) {
        if (current_page === i) {
            page_links.push(
                <button className="pagination-number active" onClick={() => {pageClick(i)}}>{i}</button>
            )
        } else {
            page_links.push(
                <button className="pagination-number" onClick={() => {pageClick(i)}}>{i}</button>
            )
        }
    }

    if (dots === 'end' || dots === 'both') {
        page_links.push(
            <span>...</span>
        )
        page_links.push(
            <button className="pagination-number" onClick={() => {pageClick(total_pages)}}>{total_pages}</button>
        )
    }

    return (
        <nav className="pagination-container">
        
        
        <div className="pagination-numbers">
        <button className="pagination-button" title="Previous page" aria-label="Previous page" onClick={() => {pageClick(current_page - 1)}}>
            &lt;
        </button>

            {page_links}

            <button className="pagination-button" title="Next page" aria-label="Next page" onClick={() => {pageClick(current_page + 1)}}>
            &gt;
        </button>
        </div>
        
        
        </nav>
    )

}

export default Paging;