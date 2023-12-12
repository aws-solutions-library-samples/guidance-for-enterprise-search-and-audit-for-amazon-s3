import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faExternalLinkAlt } from '@fortawesome/free-solid-svg-icons';
import { DateRangePicker, Modal, Button, Loader, Grid, Row, Col, Popover, Whisper } from 'rsuite';
import React from "react";
import Moment from 'moment';
import SizeDisplay from "components/SizeDisplay"
import Paging from "components/Paging"

import "./index.css";

const Results = (props) => {

    const df = 'DD/MM/YY HH:mm:ss'
    let sort_after = ''

    const [modalDetails, setModalDetails] = React.useState({
        'show': false,
        'object_name': ''
    });

    const [open, setOpen] = React.useState(false);
    const [objectDetails, setObjectDetails] = React.useState({});
    const [details, setDetails] = React.useState({});

    function handleObjectClick(temp_bucket, temp_key) {
        setObjectDetails((prevState) => {
            return {
                ...prevState,
                'bucket': temp_bucket,
                'key': temp_key
            }
        });
    }

    function handleExportClick() {
        async function fetchData() {
            const url = process.env.REACT_APP_API_GATEWAY_URL + 'export'
            const res = await fetch(url, {
                method: 'POST',
                cache: 'no-cache',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(props.searchState)
            })
        }

        fetchData().catch(console.error);
    }

    React.useEffect(() => {
        async function fetchData() {
            if (objectDetails.hasOwnProperty('bucket') && objectDetails.bucket.length) {
                const url = process.env.REACT_APP_API_GATEWAY_URL + 'object?b=' + objectDetails.bucket + '&p=' + objectDetails.key
                
                const res = await fetch(url);
                const data = await res.json();
                    
                setDetails({
                    "info": data.info,
                    "activity": data.activity
                });
            }
        }
        
        fetchData().catch(console.error);
    }, [objectDetails]);

    const handleClose = () => setOpen(false);

    const handleEntered = () => {
        
    };

    let prefix_summary, results_table
    let header_columns = {
        'aws_account' : {'sort' : 'sort'},
        'bucket' : {'sort' : 'sort'},
        'region' : {'sort' : 'sort'},
        'prefix' : {'sort' : 'sort'},
        'object_name' : {'sort' : 'sort'},
        'size' : {'sort' : 'sort'},
        'last_write' : {'sort' : 'sort'},
        'last_read' : {'sort' : 'sort'},
        'deleted' : {'sort' : 'sort'},
    }

    header_columns[props.searchState.query.sort]['sort'] = (props.searchState.query.sort_dir == 'asc') ? 'arrow-circle-up' : 'arrow-circle-down';

    function changeDirection(key_name) {
        props.setSearchState(prevState => {
            let query = Object.assign({}, prevState.query)
            query.sort_after = 
            query.sort = key_name

            // change direction if needed
            let change_in_direction = false
            if (prevState.query.sort == key_name) {
                query.sort_dir = (prevState.query.sort_dir == 'asc') ? 'desc' : 'asc';
                change_in_direction = true
            }

            // set up default sorts for columns
            switch (key_name) {
                case 'aws_account':
                    if (!change_in_direction) query.sort_dir = 'asc'
                    break;
                case 'bucket':
                    if (!change_in_direction) query.sort_dir = 'asc'
                    break;
                case 'object_name':
                    if (!change_in_direction) query.sort_dir = 'asc'
                    break;
                case 'size':
                    if (!change_in_direction) query.sort_dir = 'desc'
                    break;        
                case 'last_write':
                    if (!change_in_direction) query.sort_dir = 'desc'
                    break;        
                case 'last_read':
                    if (!change_in_direction) query.sort_dir = 'desc'
                    break;                   
                default:
                    break;
            }
            return { query };  
        })
    }

    function handleLastWrite(two_dates) {
        let date_from = Moment(two_dates[0]).format('DD/MM/YY')
        let date_to = Moment(two_dates[1]).format('DD/MM/YY')

        props.setSearchState(prevState => {
            let query = Object.assign({}, prevState.query)
            query.filters = {   
                ...query.filters,
                'last_write': date_from + '-' + date_to
            }
            return { query };
        });
    }

    function handleLastRead(two_dates) {
        let date_from = Moment(two_dates[0]).format('DD/MM/YY')
        let date_to = Moment(two_dates[1]).format('DD/MM/YY')

        props.setSearchState(prevState => {
            let query = Object.assign({}, prevState.query)
            query.filters = {   
                ...query.filters,
                'last_read': date_from + '-' + date_to
            }
            return { query };
        });
    }

    function handleFilters(event) {
        if (event.hasOwnProperty('preventDefault')) {
            event.preventDefault()
        }

        props.setSearchState(prevState => {
            let query = Object.assign({}, prevState.query)
            query.filters = {   
                ...query.filters,
                'object_name': document.getElementById('filter_object_name').value,
                'size_from': document.getElementById('filter_size_from').value,
                'size_to': document.getElementById('filter_size_to').value
            }
            return { query };
        });

        //console.log(props.searchState)
    }

    function checkToSubmit(event) {
        if (event.key === 'Enter') {
            handleFilters(event);
        }
    }

    const speaker = (<Popover title="Export Queued"><p>Export file will be sent to your S3 inventory bucket</p></Popover>);

    if ('prefixInfo' in props.searchResults && 'account' in props.searchResults.prefixInfo) {
        let s3_console_browse_link = 'https://s3.console.aws.amazon.com/s3/buckets/' + props.searchResults.prefixInfo.bucket + '?region=' + props.searchResults.prefixInfo.region + '&prefix=' + props.searchResults.prefixInfo.prefix + '/'

        prefix_summary = 
        <div id="prefix_summary">
            <div id="path">
                <div class="float-left">{props.searchResults.prefixInfo.account ? props.searchResults.prefixInfo.account:''}
                <FontAwesomeIcon icon="chevron-right" />
                {props.searchResults.prefixInfo.bucket}
                <FontAwesomeIcon icon="chevron-right" />
                {props.searchResults.prefixInfo.prefix}

                </div>
                {/* <div class="float-left pl-10">
                <a href={s3_console_browse_link} target="_blank" title="Go to S3 Console" class="pl-10">
                    <FontAwesomeIcon icon={faExternalLinkAlt} size="1x" />
                </a>
                </div> */}
                <br/>
                Objects: <strong>{props.searchResults.prefixInfo.total_count}</strong> ::: 
                Total Size: <strong><SizeDisplay size={props.searchResults.prefixInfo.total_size} /></strong> :::
                Avg Size: <strong><SizeDisplay size={props.searchResults.prefixInfo.avg_size} /></strong><br/>
                Last Write: {(props.searchResults.prefixInfo.last_write != '') ? Moment(props.searchResults.prefixInfo.last_write).format(df) : ''} :::
                Last Read: {(props.searchResults.prefixInfo.last_read != '') ? Moment(props.searchResults.prefixInfo.last_read).format(df) : ''} :::
                &nbsp;<a href="#" title="Export Inventory" class="pl-10"></a>

                <Whisper placement="top" trigger="click" controlId="control-id-click" speaker={speaker}>
                    <Button onClick={() => { handleExportClick() }}>Export Below to S3</Button>
                </Whisper>

            </div>

            <div id="prefix_info" className="row">
                <Paging totalCount={props.searchResults.prefixInfo.total_count} searchState={props.searchState} setSearchState={props.setSearchState} />
            </div>
        </div>
    } else if ('prefixInfo' in props.searchResults) {
        prefix_summary = 
        <div id="prefix_search_results">
            <h3>Search Results</h3>
            <div className="row">
                <div className="col">
                    <span>
                        Objects: <strong>{props.searchResults.prefixInfo.total_count}</strong> ::: 
                        Total Size: <strong><SizeDisplay size={props.searchResults.prefixInfo.total_size} /></strong> :::
                        Avg Size: <strong><SizeDisplay size={props.searchResults.prefixInfo.avg_size} /></strong><br/>
                        <a href="#" onClick={() => { handleExportClick() }} title="Export Inventory" class="pl-10">Export Below to S3</a>
                    </span>
                </div>
                <div className="col text-right">
                    <Paging totalCount={props.searchResults.prefixInfo.total_count} searchState={props.searchState} setSearchState={props.setSearchState} />
                </div>
            </div>
        </div>
    }

    if ('items' in props.searchResults && props.searchState.query.t == 'path') {
        results_table =
        <div id="results_table">
            <form onSubmit={handleFilters} noValidate>
                <table className="table">
                    <thead>
                        <tr>
                            <th scope="col"> </th>
                            <th scope="col">Object <FontAwesomeIcon icon={header_columns['object_name']['sort']} size="1x" onClick={() => {changeDirection('object_name')}} /></th>
                            <th scope="col">Size <FontAwesomeIcon icon={header_columns['size']['sort']} size="1x"  onClick={() => {changeDirection('size')}} /></th>
                            <th scope="col">Modified <FontAwesomeIcon icon={header_columns['last_write']['sort']} size="1x" onClick={() => {changeDirection('last_write')}} /></th>
                            <th scope="col">Read <FontAwesomeIcon icon={header_columns['last_read']['sort']} size="1x" onClick={() => {changeDirection('last_read')}} /></th>
                            <th scope="col">Deleted <FontAwesomeIcon icon={header_columns['deleted']['sort']} size="1x" onClick={() => {changeDirection('deleted')}} /></th>
                        </tr>
                        <tr>
                            <th scope="col"> </th>
                            <th scope="col">
                                <input type="text" placeholder="Filter" id="filter_object_name" onKeyUp={checkToSubmit} defaultValue={('object_name' in props.searchState.query.filters) ? props.searchState.query.filters.object_name : ''} />
                            </th>
                            <th scope="col">
                                <input type="text" placeholder="From" id="filter_size_from" onKeyUp={checkToSubmit} defaultValue={('size_from' in props.searchState.query.filters) ? props.searchState.query.filters.size_from : ''} />
                                &nbsp;-&nbsp;
                                <input type="text" placeholder="To" id="filter_size_to" onKeyUp={checkToSubmit} defaultValue={('size_to' in props.searchState.query.filters) ? props.searchState.query.filters.size_to : ''} />
                                &nbsp;KB
                            </th>
                            <th scope="col"><DateRangePicker format="dd/MM/yy" character=" - " placeholder="Date Range" onOk={handleLastWrite} /></th>
                            <th scope="col"><DateRangePicker format="dd/MM/yy" character=" - " placeholder="Date Range" onOk={handleLastRead}  /></th>
                        </tr>
                    </thead>
                    <tbody>
                        {props.searchResults.items.map((item) => {
                            let full_key = ((item.hasOwnProperty('prefix') && item['prefix'] != '') ? item['prefix'] + '/' : '') + item['object_name']
                            let s3_console_link = 'https://s3.console.aws.amazon.com/s3/object/' + item['bucket'] + '?region=' + item['region'] + '&prefix='
                            if (item['prefix'].length) { s3_console_link += item['prefix'] + '/' }
                            s3_console_link += item['object_name']
                            return (
                                <tr>
                                    <td>
                                        <a href={s3_console_link} target="_blank" title="Go to S3 Console">
                                        <FontAwesomeIcon icon={faExternalLinkAlt} size="1x" />
                                        </a>
                                    </td>
                                    <td><a href="#" onClick={() => { handleObjectClick(item['bucket'],full_key); setOpen(true); return false;} }>{item['object_name']}</a></td>
                                    <td><SizeDisplay size={item['size']} /></td>
                                    <td>{(item['last_write'] != '') ? Moment(item['last_write']).format(df) : ''}</td>
                                    <td>{(item['last_read'] != '') ? Moment(item['last_read']).format(df) : ''}</td>
                                    <td>{(item['deleted'] === true) ? 'Yes' : 'No'}</td>
                                </tr>
                            )
                        })}
                    </tbody>
                </table>
            </form>
        </div>
    }

    if ('items' in props.searchResults && props.searchState.query.t == 'search') {
        results_table =
        <div id="results_table">
            <table className="table">
                <thead>
                    <tr>
                        <th scope="col"> </th>
                        <th scope="col">Account <FontAwesomeIcon icon={header_columns['aws_account']['sort']} size="1x" onClick={() => {changeDirection('aws_account')}} /></th>
                        <th scope="col">Region <FontAwesomeIcon icon={header_columns['region']['sort']} size="1x" onClick={() => {changeDirection('region')}} /></th>
                        <th scope="col">Bucket <FontAwesomeIcon icon={header_columns['bucket']['sort']} size="1x" onClick={() => {changeDirection('bucket')}} /></th>
                        <th scope="col">Path <FontAwesomeIcon icon={header_columns['prefix']['sort']} size="1x" onClick={() => {changeDirection('prefix')}} /></th>
                        <th scope="col">Object <FontAwesomeIcon icon={header_columns['object_name']['sort']} size="1x" onClick={() => {changeDirection('object_name')}} /></th>
                        <th scope="col">Size <FontAwesomeIcon icon={header_columns['size']['sort']} size="1x"  onClick={() => {changeDirection('size')}} /></th>
                        <th scope="col">Modified <FontAwesomeIcon icon={header_columns['last_write']['sort']} size="1x" onClick={() => {changeDirection('last_write')}} /></th>
                        <th scope="col">Read <FontAwesomeIcon icon={header_columns['last_read']['sort']} size="1x" onClick={() => {changeDirection('last_read')}} /></th>
                        <th scope="col">Deleted <FontAwesomeIcon icon={header_columns['deleted']['sort']} size="1x" onClick={() => {changeDirection('deleted')}} /></th>
                        {/* {props.searchState.query.q.indexOf(':') > 0 ? (
                            <th scope="col">Tags</th>
                        ) : (
                            <th scope="col"></th>
                        )} */}
                    </tr>
                    {/*<tr>
                        <th scope="col"> </th>
                        <th scope="col">
                            <input type="text" placeholder="Filter" id="filter_aws_account" onKeyUp={checkToSubmit} defaultValue={('aws_account' in props.searchState.query.filters) ? props.searchState.query.filters.aws_account : ''} />
                        </th>
                        <th scope="col">
                            <input type="text" placeholder="Filter" id="filter_region" onKeyUp={checkToSubmit} defaultValue={('region' in props.searchState.query.filters) ? props.searchState.query.filters.region : ''} />
                        </th>

                        <th scope="col">
                            <input type="text" placeholder="From" id="filter_size_from" onKeyUp={checkToSubmit} defaultValue={('size_from' in props.searchState.query.filters) ? props.searchState.query.filters.size_from : ''} />
                            &nbsp;-&nbsp;
                            <input type="text" placeholder="To" id="filter_size_to" onKeyUp={checkToSubmit} defaultValue={('size_to' in props.searchState.query.filters) ? props.searchState.query.filters.size_to : ''} />
                            &nbsp;KB
                        </th>
                        <th scope="col"><DateRangePicker format="dd/MM/yy" character=" - " placeholder="Date Range" onOk={handleLastWrite} /></th>
                        <th scope="col"><DateRangePicker format="dd/MM/yy" character=" - " placeholder="Date Range" onOk={handleLastRead}  /></th>
                    </tr>*/}
                </thead>
                <tbody>
                    {props.searchResults.items.map((item) => {
                        let full_key = ((item.hasOwnProperty('prefix') && item['prefix'] != '') ? item['prefix'] + '/' : '') + item['object_name']
                        let s3_console_link = 'https://s3.console.aws.amazon.com/s3/object/' + item['bucket'] + '?region=' + item['region'] + '&prefix='
                        if (item['prefix'].length) { s3_console_link += item['prefix'] + '/' }
                        s3_console_link += item['object_name']
                        return (
                            <tr>
                                <td>
                                    <a href={s3_console_link} target="_blank" title="Go to S3 Console">
                                    <FontAwesomeIcon icon={faExternalLinkAlt} size="1x" />
                                    </a>
                                </td>
                                <td>{item['account']}</td>
                                <td>{item['region']}</td>
                                <td>{item['bucket']}</td>
                                <td>{item['prefix']}</td>
                                <td><a href="#" onClick={() => { handleObjectClick(item['bucket'],full_key); setOpen(true); return false;} }>{item['object_name']}</a></td>
                                <td><SizeDisplay size={item['size']} /></td>
                                <td>{(item['last_write'] != '') ? Moment(item['last_write']).format(df) : ''}</td>
                                <td>{(item['last_read'] != '') ? Moment(item['last_read']).format(df) : ''}</td>
                                <td>{(item['deleted'] === true) ? 'Yes' : 'No'}</td>
                                {/* {props.searchState.query.q.indexOf(':') > 0 ? (
                                    <td>{(item['tags'] != '') ? item['tags'].map(tag) : ''}</td>
                                ) : (
                                    <td></td>
                                )} */}
                            </tr>
                        )
                    })
                    

                    }
                </tbody>
            </table>

            <Paging totalCount={props.searchResults.prefixInfo.total_count} searchState={props.searchState} setSearchState={props.setSearchState} />
        </div>
    }

    return (
        <div>
            {prefix_summary}
            {results_table}

            <Modal open={open} onClose={handleClose} onEntered={handleEntered} onExited={() => {setObjectDetails({})}}>
                <Modal.Body>
                    {details.hasOwnProperty('info') ? (
                        <>
                            <h5>Object Summary</h5>
                            <Grid fluid>
                                <Row className="show-grid"><Col xs={6}><strong>AWS Account</strong></Col><Col xs={18}>{details.info.account}</Col></Row>
                                <Row className="show-grid"><Col xs={6}><strong>Region</strong></Col><Col xs={18}>{details.info.region}</Col></Row>
                                <Row className="show-grid"><Col xs={6}><strong>Bucket</strong></Col><Col xs={18}>{details.info.bucket}</Col></Row>
                                <Row className="show-grid"><Col xs={6}><strong>Prefix</strong></Col><Col xs={18}>{details.info.prefix}</Col></Row>
                                <Row className="show-grid"><Col xs={6}><strong>Name</strong></Col><Col xs={18}>{details.info.object_name}</Col></Row>
                                <Row className="show-grid"><Col xs={6}><strong>Size</strong></Col><Col xs={18}><SizeDisplay size={details.info.size} /></Col></Row>
                                <Row className="show-grid"><Col xs={6}><strong>Modified</strong></Col><Col xs={18}>{(details.info.last_write != '') ? Moment(details.info.last_write).format(df) : ''}</Col></Row>
                                <Row className="show-grid"><Col xs={6}><strong>Latest Read</strong></Col><Col xs={18}>{(details.info.last_read != '') ? Moment(details.info.last_read).format(df) : ''}</Col></Row>
                                <Row className="show-grid"><Col xs={6}><strong>Storage Class</strong></Col><Col xs={18}>{details.info.storage_class}</Col></Row>
                                <Row className="show-grid"><Col xs={6}><strong>Encryption</strong></Col><Col xs={18}>{details.info.sse_encryption}</Col></Row>
                                <Row className="show-grid"><Col xs={6}><strong>Etag</strong></Col><Col xs={18}>{details.info.etag}</Col></Row>
                            </Grid>

                            <br/>
                            <h5>Tags</h5>
                            <Grid fluid>
                                {details.info.tags.map((data, idx) => (
                                    Object.keys(data).map(key => (
                                       <Row className="show-grid"><Col xs={6}><strong>{key}</strong></Col><Col xs={18}>{data[key]}</Col></Row>
                                    ))
                                ))}
                            </Grid>

                            <br/>
                            <h5>Access Activity</h5>
                            <Grid fluid>
                                {details.activity.map((data, idx) => (
                                    <Row className="show-grid">
                                        <Col xs={6}><strong>Access Date</strong></Col>
                                        <Col xs={18}>{(data['last_read'] != '') ? Moment(data['last_read']).format(df) : ''}</Col>
                                    </Row>
                                ))}
                            </Grid>
                        </>
                    ) : (
                        <div style={{ textAlign: 'center' }}>
                            <Loader size="md" />
                        </div>
                    )}
                </Modal.Body>
                <Modal.Footer>
                    <Button onClick={handleClose} appearance="subtle">Close</Button>
                </Modal.Footer>
            </Modal>
        </div>
    )

}

export default Results;