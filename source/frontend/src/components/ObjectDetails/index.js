import React, { useState } from 'react';
import Button from 'react-bootstrap/Button';
import Modal from 'react-bootstrap/Modal';

const ObjectDetails = (props) => {
  const [lgShow, setLgShow] = useState(false);

  return (
    <>
      <Button onClick={() => setLgShow(true)}>Large modal</Button>

      <Modal
        size="lg"
        show={lgShow}
        onHide={() => setLgShow(false)}
        aria-labelledby="example-modal-sizes-title-lg"
      >
        <Modal.Header closeButton>
          <Modal.Title id="example-modal-sizes-title-lg">
            Large Modal
          </Modal.Title>
        </Modal.Header>
        <Modal.Body>...</Modal.Body>
      </Modal>
    </>
  );
}

export default ObjectDetails;