import logging
import os

from flask import Flask, render_template, redirect, url_for, request, flash
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

_secret_key = os.environ.get('SECRET_KEY')
if not _secret_key:
    logging.warning(
        "SECRET_KEY environment variable is not set. "
        "Using the insecure default 'dev' key. "
        "Set SECRET_KEY in production."
    )
    _secret_key = 'dev'
app.config['SECRET_KEY'] = _secret_key
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///baremgr.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

CATEGORIES = ['Spirits', 'Wine', 'Beer', 'Soft Drinks', 'Mixers', 'Other']
LOW_STOCK_THRESHOLD = 5


class Item(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    category = db.Column(db.String(50), nullable=False)
    quantity = db.Column(db.Integer, nullable=False, default=0)
    price = db.Column(db.Float, nullable=False, default=0.0)
    unit = db.Column(db.String(30), nullable=False, default='bottle')

    def __repr__(self):
        return f'<Item {self.name}>'


def create_tables():
    with app.app_context():
        db.create_all()


@app.route('/')
def index():
    items = Item.query.order_by(Item.category, Item.name).all()
    return render_template('index.html', items=items, threshold=LOW_STOCK_THRESHOLD)


@app.route('/add', methods=['GET', 'POST'])
def add_item():
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        category = request.form.get('category', '')
        unit = request.form.get('unit', '').strip()

        try:
            quantity = int(request.form.get('quantity', 0))
            price = float(request.form.get('price', 0.0))
        except ValueError:
            flash('Quantity must be a whole number and price must be a number.', 'danger')
            return render_template('item_form.html', categories=CATEGORIES, action='Add', item=None)

        if not name:
            flash('Item name is required.', 'danger')
            return render_template('item_form.html', categories=CATEGORIES, action='Add', item=None)

        if category not in CATEGORIES:
            flash('Invalid category.', 'danger')
            return render_template('item_form.html', categories=CATEGORIES, action='Add', item=None)

        if quantity < 0:
            flash('Quantity cannot be negative.', 'danger')
            return render_template('item_form.html', categories=CATEGORIES, action='Add', item=None)

        if price < 0:
            flash('Price cannot be negative.', 'danger')
            return render_template('item_form.html', categories=CATEGORIES, action='Add', item=None)

        item = Item(name=name, category=category, quantity=quantity, price=price, unit=unit)
        db.session.add(item)
        db.session.commit()
        flash(f'"{name}" added successfully.', 'success')
        return redirect(url_for('index'))

    return render_template('item_form.html', categories=CATEGORIES, action='Add', item=None)


@app.route('/edit/<int:item_id>', methods=['GET', 'POST'])
def edit_item(item_id):
    item = db.get_or_404(Item, item_id)

    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        category = request.form.get('category', '')
        unit = request.form.get('unit', '').strip()

        try:
            quantity = int(request.form.get('quantity', 0))
            price = float(request.form.get('price', 0.0))
        except ValueError:
            flash('Quantity must be a whole number and price must be a number.', 'danger')
            return render_template('item_form.html', categories=CATEGORIES, action='Edit', item=item)

        if not name:
            flash('Item name is required.', 'danger')
            return render_template('item_form.html', categories=CATEGORIES, action='Edit', item=item)

        if category not in CATEGORIES:
            flash('Invalid category.', 'danger')
            return render_template('item_form.html', categories=CATEGORIES, action='Edit', item=item)

        if quantity < 0:
            flash('Quantity cannot be negative.', 'danger')
            return render_template('item_form.html', categories=CATEGORIES, action='Edit', item=item)

        if price < 0:
            flash('Price cannot be negative.', 'danger')
            return render_template('item_form.html', categories=CATEGORIES, action='Edit', item=item)

        item.name = name
        item.category = category
        item.quantity = quantity
        item.price = price
        item.unit = unit
        db.session.commit()
        flash(f'"{name}" updated successfully.', 'success')
        return redirect(url_for('index'))

    return render_template('item_form.html', categories=CATEGORIES, action='Edit', item=item)


@app.route('/delete/<int:item_id>', methods=['POST'])
def delete_item(item_id):
    item = db.get_or_404(Item, item_id)
    name = item.name
    db.session.delete(item)
    db.session.commit()
    flash(f'"{name}" deleted successfully.', 'success')
    return redirect(url_for('index'))


if __name__ == '__main__':
    create_tables()
    app.run(debug=os.environ.get('FLASK_DEBUG', 'false').lower() == 'true')
