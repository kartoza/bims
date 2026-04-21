import React from 'react';
import '../css/SourceReference.scss';
import DOI from "./components/DOI";
import Author from "./components/Author";


const PEER_REVIEWED = 'Peer-reviewed scientific article'
const PUBLISHED_REPORT = 'Published book, report or thesis'
const DATABASE = 'Database'
const UNPUBLISHED = 'Unpublished'
const REPORT_SOURCE = ['doc_url', 'doc_file']

class AddSourceReferenceView extends React.Component {
  constructor(props){
    super(props);
    this.state = {
      selected_reference_type: this.props.reference_type[0],
      authors: []
    };
    this.authorInput = null;
    this.doiInput = null;
    this.handleChange = this.handleChange.bind(this);
    this.handleSubmit = this.handleSubmit.bind(this);
    this.field = this.field.bind(this);
    this.updateForm = this.updateForm.bind(this);
    this.handleInputChange = this.handleInputChange.bind(this);
    this.handleReportSourceChange = this.handleReportSourceChange.bind(this);
  }

  handleChange(event) {
    this.setState({
      selected_reference_type: event.target.value,
      name: '',
      title: '',
      source: '',
      description: '',
      url: '',
      year: '',
      source_date: '',
      notes: '',
      report_source: REPORT_SOURCE[1],
      authors: []
    });
  }

  checkRequiredFields() {
    switch (this.state.selected_reference_type) {
      case PUBLISHED_REPORT:
      case PEER_REVIEWED:
        return this.state.source && this.state.title && this.state.year
      default:
        return true
    }
  }

  handleSubmit(event) {
    event.preventDefault();

    // Update authors
    if (this.authorInput) {
      const authorIds = [];
      const authorNames = [];
      for (let i=0; i < this.authorInput.state.authors.length; i++) {
        const author = this.authorInput.state.authors[i];
        if (author.id) {
          authorIds.push(author.id)
        } else if (author.first_name) {
          authorNames.push(`${author.first_name} ${author.last_name}`);
        } else {
          authorNames.push(author)
        }
      }
      document.getElementById('author_ids').value = authorIds.join();
      document.getElementById('author_names').value = authorNames.join();
    }

    if (this.doiInput && this.doiInput.state.entry) {
      if (this.doiInput.state.entry.data_exist) {
        alert('Data already exists.')
        return;
      }
    }

    const valid = this.checkRequiredFields();
    if (!valid) {
      alert('Missing required fields.')
      return;
    }

    document.getElementById('source_reference_form').submit();
  }

  handleInputChange(field, event) {
    let { value, maxLength } = event.target;
    if (maxLength > 0) {
       value = value.slice(0, maxLength);
    }
    this.setState({
      [field]: value
    })
  }

  handleReportSourceChange(event) {
    this.setState({
      report_source: event.target.value
    });
  }

  field(field) {
    switch (field) {
      case 'doi':
        return this.state.selected_reference_type === PEER_REVIEWED
      case 'author':
        return this.state.selected_reference_type === PEER_REVIEWED ||
            this.state.selected_reference_type === PUBLISHED_REPORT ||
            this.state.selected_reference_type === DATABASE ||
            this.state.selected_reference_type === UNPUBLISHED
      case 'year':
      case 'title':
        return this.state.selected_reference_type === PEER_REVIEWED ||
            this.state.selected_reference_type === PUBLISHED_REPORT
      case 'file':
        return this.state.selected_reference_type === PUBLISHED_REPORT
      case 'source':
        return this.state.selected_reference_type === PUBLISHED_REPORT ||
            this.state.selected_reference_type === UNPUBLISHED ||
            this.state.selected_reference_type === PEER_REVIEWED ||
            this.state.selected_reference_type === DATABASE
      case 'date':
        return this.state.selected_reference_type === DATABASE ||
            this.state.selected_reference_type === UNPUBLISHED
      case 'description':
      case 'url':
      case 'name':
        return this.state.selected_reference_type === DATABASE
      default:
        return false
    }
  }

  updateForm(formData) {
    this.setState(formData)
  }

  render() {
    return (
      <form encType="multipart/form-data" name="source_reference_form" method="post" id="source_reference_form">
        <input type="hidden" name="csrfmiddlewaretoken" value={this.props.csrfToken} />
        <input type="hidden" name="author_ids" id="author_ids" />
        <input type="hidden" name="author_names" id="author_names" />
        <div className="form-group">
          <label>Type</label>
          <select name="reference_type" className="form-control" value={this.state.selected_reference_type} onChange={this.handleChange}>
            {this.props.reference_type.map((reference_type) =>
              <option value={reference_type}>{ reference_type }</option>
            )}
          </select>
        </div>
        {/*DOI*/}
        {this.field('doi') ?
          <DOI updateForm={this.updateForm} ref={(doiInput) => { this.doiInput = doiInput }} /> : null
        }
        {
          this.field('file') ?
            <div>
              <div className="form-check">
                <input className="form-check-input" type="radio"
                       name="doc_type" id="provide-url" value="doc_url" checked={this.state.report_source === 'doc_url'}
                       onChange={(e) => this.handleReportSourceChange(e)} />
                  <label className="form-check-label" htmlFor="provide-url">
                    Provide url
                  </label>
              </div>
              <div className="form-check">
                <input className="form-check-input" type="radio"
                       name="doc_type" id="upload-file" value="doc_file" checked={this.state.report_source === 'doc_file'}
                       onChange={(e) => this.handleReportSourceChange(e)} />
                  <label className="form-check-label" htmlFor="upload-file">
                    Upload a file
                  </label>
              </div>

              <div className="form-group">
                {this.state.report_source === 'doc_file' ?
                    <input type="file" className="form-control"
                           accept="application/pdf"
                           name="report_file"
                           id="report_file" aria-describedby="fileHelp"/>
                   : <input id="document-url" type="text"
                            placeholder="Enter url"
                            className="form-control form-control-sm" name="report_url"/> }
              </div>

            </div>
          : null
        }

        {this.field('name') ?
        <div className="form-group required-input">
          <label>Name</label>
          <input type="text" className="form-control"
                 name="name" id="name" aria-describedby="titleHelp" required
                 placeholder="Enter Name" value={this.state.name} onChange={(e) => this.handleInputChange('name', e)}/>
        </div> : null }

        {this.field('title') ?
        <div className="form-group required-input">
          <label>Title</label>
          <input type="text" className="form-control"
                 name="title" id="title" aria-describedby="titleHelp" required
                 placeholder="Enter Title" value={this.state.title} onChange={(e) => this.handleInputChange('title', e)}/>
        </div> : null }

        {this.field('source') ?
        <div className={`form-group${this.state.selected_reference_type !== DATABASE ? ' required-input' : ''}`}>
          <label>{ this.state.selected_reference_type === DATABASE ? 'Source name' : 'Source' }</label>
          <input type="text" className="form-control"
                 name="source" id="source" aria-describedby="sourceHelp"
                 placeholder={ this.state.selected_reference_type === DATABASE ? 'Enter Source name' : 'Enter Source' }
                 value={this.state.source}  onChange={(e) => this.handleInputChange('source', e)}/>
        </div> : null }

        {this.field('description') ?
        <div className="form-group">
          <label>Description</label>
          <input type="text" className="form-control"
                 name="description" id="description" aria-describedby="sourceHelp"
                 placeholder="Enter Description" value={this.state.description}  onChange={(e) => this.handleInputChange('description', e)}/>
        </div> : null }

        {this.field('url') ?
        <div className="form-group">
          <label>URL</label>
          <input type="text" className="form-control"
                 name="url" id="url" aria-describedby="urlHelp"
                 placeholder="Enter URL" value={this.state.url}  onChange={(e) => this.handleInputChange('url', e)}/>
        </div> : null }

        {this.field('year') ?
          <div className="form-group required-input">
            <label>Year</label>
            <input type="number" className="form-control" maxLength="4"
                   name="year" id="year" aria-describedby="yearHelp"
                   placeholder="Enter Year"  value={this.state.year} onChange={(e) => this.handleInputChange('year', e)} />
          </div> : null
        }

        {this.field('date') ?
          <div className="form-group">
            <label>Date</label>
            <input type="date" className="form-control"
                   name="source_date" id="source_date"
                   value={this.state.source_date || ''} onChange={(e) => this.handleInputChange('source_date', e)} />
          </div> : null
        }

        <div className="form-group">
          <label>{ this.state.selected_reference_type === UNPUBLISHED ? "Title" : "Notes" }</label>
          <input type="text" className="form-control"
                 name="notes" id="notes" aria-describedby="urlHelp"
                 placeholder={ this.state.selected_reference_type === UNPUBLISHED ? "Enter Title" : "Enter Notes" }
                 value={this.state.notes}  onChange={(e) => this.handleInputChange('notes', e)}/>
        </div>

        { this.field('author') ? <Author authors={this.state.authors} ref={(author) => { this.authorInput = author }} /> : null }
        <button type="submit" className="btn btn-primary" onClick={this.handleSubmit}>Submit</button>
      </form>
    );
  }
}

$(function (){
  $("[data-addsourcereferenceview]").each(function(){
      let props = $(this).data()
      props.history = history;
      delete(props.addsourcereferenceview);
      window.ReactDOM.render(<AddSourceReferenceView {...props}/>, $(this).get(0));
  });
})

export default AddSourceReferenceView;
