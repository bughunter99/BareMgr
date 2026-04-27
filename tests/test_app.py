import pytest
from app import app, db, Item


@pytest.fixture
def client():
    app.config['TESTING'] = True
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['WTF_CSRF_ENABLED'] = False

    with app.app_context():
        db.create_all()
        yield app.test_client()
        db.session.remove()
        db.drop_all()


def test_index_empty(client):
    """GET / returns 200 and shows empty state."""
    response = client.get('/')
    assert response.status_code == 200
    assert b'No items yet' in response.data


def test_add_item(client):
    """POST /add creates a new item and redirects to index."""
    response = client.post('/add', data={
        'name': 'Hendricks Gin',
        'category': 'Spirits',
        'quantity': 10,
        'unit': 'bottle',
        'price': 35.99,
    }, follow_redirects=True)
    assert response.status_code == 200
    assert b'Hendricks Gin' in response.data
    assert b'added successfully' in response.data


def test_add_item_missing_name(client):
    """POST /add with no name shows an error."""
    response = client.post('/add', data={
        'name': '',
        'category': 'Spirits',
        'quantity': 5,
        'unit': 'bottle',
        'price': 20.00,
    }, follow_redirects=True)
    assert response.status_code == 200
    assert b'required' in response.data


def test_list_items(client):
    """GET / lists items stored in the database."""
    with app.app_context():
        db.session.add(Item(name='Merlot', category='Wine', quantity=8, price=12.50, unit='bottle'))
        db.session.add(Item(name='Lager', category='Beer', quantity=24, price=2.00, unit='can'))
        db.session.commit()

    response = client.get('/')
    assert response.status_code == 200
    assert b'Merlot' in response.data
    assert b'Lager' in response.data


def test_low_stock_highlight(client):
    """Items with quantity <= 5 are flagged as low stock."""
    with app.app_context():
        db.session.add(Item(name='Tonic Water', category='Mixers', quantity=3, price=1.50, unit='can'))
        db.session.commit()

    response = client.get('/')
    assert response.status_code == 200
    assert b'Low Stock' in response.data
    assert b'low-stock' in response.data


def test_edit_item_get(client):
    """GET /edit/<id> returns the pre-filled form."""
    with app.app_context():
        item = Item(name='Vodka', category='Spirits', quantity=6, price=25.00, unit='bottle')
        db.session.add(item)
        db.session.commit()
        item_id = item.id

    response = client.get(f'/edit/{item_id}')
    assert response.status_code == 200
    assert b'Vodka' in response.data


def test_edit_item_post(client):
    """POST /edit/<id> updates the item and redirects."""
    with app.app_context():
        item = Item(name='Rum', category='Spirits', quantity=4, price=18.00, unit='bottle')
        db.session.add(item)
        db.session.commit()
        item_id = item.id

    response = client.post(f'/edit/{item_id}', data={
        'name': 'Dark Rum',
        'category': 'Spirits',
        'quantity': 7,
        'unit': 'bottle',
        'price': 22.00,
    }, follow_redirects=True)
    assert response.status_code == 200
    assert b'Dark Rum' in response.data
    assert b'updated successfully' in response.data


def test_delete_item(client):
    """POST /delete/<id> removes the item from the database."""
    with app.app_context():
        item = Item(name='Amaretto', category='Spirits', quantity=2, price=30.00, unit='bottle')
        db.session.add(item)
        db.session.commit()
        item_id = item.id

    response = client.post(f'/delete/{item_id}', follow_redirects=True)
    assert response.status_code == 200
    assert b'deleted successfully' in response.data

    with app.app_context():
        assert db.session.get(Item, item_id) is None


def test_edit_nonexistent_item(client):
    """GET /edit/<id> for a missing item returns 404."""
    response = client.get('/edit/9999')
    assert response.status_code == 404


def test_delete_nonexistent_item(client):
    """POST /delete/<id> for a missing item returns 404."""
    response = client.post('/delete/9999')
    assert response.status_code == 404
