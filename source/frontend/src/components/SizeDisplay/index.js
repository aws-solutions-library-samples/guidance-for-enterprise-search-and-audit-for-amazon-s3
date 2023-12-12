import React from "react";

const SizeDisplay = (props) => {

    function easyRead(x) {
        return Number.parseFloat(x).toFixed(2);
    }

    let size_string = ''
    if (props.size < 1024) {
        size_string = props.size.toString() + ' B'
    } else if (props.size < 1024000 ) {
        size_string = easyRead(props.size / 1024).toString() + ' KB'
    } else if (props.size < 1024000000 ) {
        size_string = easyRead(props.size / 1024 / 1024).toString() + ' MB'
    } else if (props.size < 1024000000000 ) {
        size_string = easyRead(props.size / 1024 / 1024 / 1024).toString() + ' GB'
    } else {
        size_string = easyRead(props.size / 1024 / 1024 / 1024 / 1024).toString() + ' TB'
    }


    return (
        <span>
            {size_string}
        </span>
    )

}

export default SizeDisplay;