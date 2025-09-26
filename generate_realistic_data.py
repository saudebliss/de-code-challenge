#!/usr/bin/env python3
"""
Script para gerar dados mais realistas e complexos para o desafio de Data Engineering.

Melhorias implementadas:
1. Dados mais realistas com IDs únicos (UUIDs)
2. Cenários de inadimplência mais desafiadores
3. Estrutura de dados mais rica
"""

import json
import uuid
import random
from datetime import datetime, timedelta
from decimal import Decimal
import faker

# Configurar faker para dados brasileiros
fake = faker.Faker('pt_BR')

# Configurações
NUM_CUSTOMERS = 150
NUM_TRANSACTIONS = 500
PAYMENT_METHODS = ['credit_card', 'debit_card', 'pix', 'bank_transfer', 'cash', 'boleto']
CURRENCIES = ['BRL', 'USD', 'EUR']
PRODUCT_CATEGORIES = ['electronics', 'clothing', 'books', 'home', 'sports', 'automotive', 'health']
TRANSACTION_STATUSES = ['pending', 'completed', 'cancelled', 'refunded']

def generate_customer_ids():
    """Gera IDs únicos de clientes"""
    return [f"cust_{uuid.uuid4().hex[:8]}" for _ in range(NUM_CUSTOMERS)]

def generate_realistic_transactions(customer_ids):
    """Gera transações mais realistas e complexas"""
    transactions = []
    
    for i in range(NUM_TRANSACTIONS):
        # Data da transação entre 18 meses atrás e hoje
        transaction_date = fake.date_between(start_date='-18M', end_date='today')
        
        # Data de vencimento entre 15 e 90 dias após a transação
        due_days = random.choice([15, 30, 45, 60, 90])
        due_date = transaction_date + timedelta(days=due_days)
        
        # Valor da transação com distribuição mais realista
        if random.random() < 0.7:  # 70% valores baixos/médios
            amount = round(random.uniform(50.0, 2000.0), 2)
        elif random.random() < 0.9:  # 20% valores altos
            amount = round(random.uniform(2000.0, 10000.0), 2)
        else:  # 10% valores muito altos
            amount = round(random.uniform(10000.0, 50000.0), 2)
        
        # Alguns clientes têm múltiplas transações (comportamento realista)
        if random.random() < 0.3:  # 30% chance de cliente repetido
            customer_id = random.choice(customer_ids[:50])  # Concentrar em alguns clientes
        else:
            customer_id = random.choice(customer_ids)
        
        transaction = {
            "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
            "customer_id": customer_id,
            "amount": amount,
            "currency": random.choices(CURRENCIES, weights=[0.85, 0.10, 0.05])[0],  # 85% BRL
            "transaction_date": transaction_date.isoformat(),
            "due_date": due_date.isoformat(),
            "payment_terms": f"{due_days}_days",
            "status": random.choices(TRANSACTION_STATUSES, weights=[0.7, 0.25, 0.03, 0.02])[0],
            "metadata": {
                "product_category": random.choice(PRODUCT_CATEGORIES),
                "sales_rep": f"rep_{random.randint(1, 20):03d}",
                "channel": random.choice(['online', 'store', 'phone', 'mobile_app']),
                "discount_applied": random.choice([0, 5, 10, 15, 20]) if random.random() < 0.3 else 0,
                "installments": random.choice([1, 2, 3, 6, 12]) if random.random() < 0.4 else 1
            }
        }
        
        # Adicionar alguns dados inconsistentes para testar limpeza (5% dos casos)
        if random.random() < 0.05:
            if random.random() < 0.5:
                transaction["amount"] = None  # Valor nulo
            else:
                transaction["currency"] = ""  # Moeda vazia
        
        transactions.append(transaction)
    
    return transactions

def generate_complex_payments(transactions):
    """Gera pagamentos com cenários mais complexos e desafiadores"""
    payments = []
    
    for transaction in transactions:
        if transaction["status"] == "cancelled":
            continue  # Transações canceladas não têm pagamentos
        
        transaction_date = datetime.fromisoformat(transaction["transaction_date"])
        due_date = datetime.fromisoformat(transaction["due_date"])
        amount = transaction["amount"]
        
        if amount is None:  # Pular transações com valor nulo
            continue
        
        # Cenários de pagamento
        payment_scenario = random.choices([
            'full_on_time',      # Pagamento completo no prazo
            'full_late',         # Pagamento completo atrasado
            'partial_multiple',  # Pagamentos parciais múltiplos
            'early_payment',     # Pagamento antecipado
            'no_payment',        # Sem pagamento (inadimplente)
            'overpayment'        # Pagamento a maior
        ], weights=[0.45, 0.20, 0.15, 0.08, 0.10, 0.02])[0]
        
        if payment_scenario == 'no_payment':
            continue  # Sem pagamento
        
        elif payment_scenario == 'full_on_time':
            # Pagamento completo entre a data da transação e vencimento
            payment_date = fake.date_between(start_date=transaction_date, end_date=due_date)
            payments.append(create_payment(transaction, amount, payment_date))
        
        elif payment_scenario == 'full_late':
            # Pagamento atrasado (1 a 120 dias após vencimento)
            delay_days = random.choices([
                random.randint(1, 15),   # Atraso pequeno
                random.randint(16, 30),  # Atraso médio
                random.randint(31, 60),  # Atraso grande
                random.randint(61, 120)  # Atraso muito grande
            ], weights=[0.4, 0.3, 0.2, 0.1])[0]
            
            payment_date = due_date + timedelta(days=delay_days)
            payments.append(create_payment(transaction, amount, payment_date))
        
        elif payment_scenario == 'early_payment':
            # Pagamento antecipado (até 30 dias antes do vencimento)
            early_days = random.randint(1, min(30, (due_date - transaction_date).days))
            payment_date = due_date - timedelta(days=early_days)
            payments.append(create_payment(transaction, amount, payment_date))
        
        elif payment_scenario == 'partial_multiple':
            # Múltiplos pagamentos parciais
            remaining_amount = amount
            payment_count = random.randint(2, 5)
            current_date = transaction_date
            
            for i in range(payment_count):
                if i == payment_count - 1:  # Último pagamento
                    payment_amount = remaining_amount
                else:
                    # Pagamento parcial (10% a 60% do restante)
                    payment_amount = round(remaining_amount * random.uniform(0.1, 0.6), 2)
                    remaining_amount -= payment_amount
                
                # Data do pagamento com possível atraso
                if random.random() < 0.3:  # 30% chance de atraso
                    delay = random.randint(1, 45)
                    payment_date = current_date + timedelta(days=random.randint(15, 30) + delay)
                else:
                    payment_date = current_date + timedelta(days=random.randint(15, 30))
                
                payments.append(create_payment(transaction, payment_amount, payment_date, f"partial_{i+1}"))
                current_date = payment_date
        
        elif payment_scenario == 'overpayment':
            # Pagamento a maior (erro ou devolução posterior)
            overpay_amount = round(amount * random.uniform(1.05, 1.5), 2)
            payment_date = fake.date_between(start_date=transaction_date, end_date=due_date + timedelta(days=30))
            payments.append(create_payment(transaction, overpay_amount, payment_date))
    
    return payments

def create_payment(transaction, amount, payment_date, payment_type="full"):
    """Cria um registro de pagamento"""
    return {
        "payment_id": f"pay_{uuid.uuid4().hex[:12]}",
        "transaction_id": transaction["transaction_id"],
        "customer_id": transaction["customer_id"],
        "amount_paid": amount,
        "currency": transaction["currency"],
        "payment_date": payment_date.isoformat(),
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_type": payment_type,
        "status": random.choices(['completed', 'processing', 'failed'], weights=[0.92, 0.05, 0.03])[0],
        "metadata": {
            "processor": random.choice(['stripe', 'paypal', 'cielo', 'stone', 'mercadopago']),
            "fee_amount": round(amount * random.uniform(0.01, 0.05), 2),
            "reference_number": f"ref_{random.randint(100000, 999999)}",
            "ip_address": fake.ipv4() if random.random() < 0.8 else None,
            "user_agent": fake.user_agent() if random.random() < 0.7 else None
        }
    }

def add_data_quality_issues(data, issue_rate=0.03):
    """Adiciona problemas de qualidade de dados para testar robustez"""
    for item in data:
        if random.random() < issue_rate:
            issue_type = random.choice([
                'missing_field', 'wrong_type', 'invalid_date', 'negative_amount'
            ])
            
            if issue_type == 'missing_field':
                # Remove um campo aleatório
                fields = list(item.keys())
                if fields:
                    del item[random.choice(fields)]
            
            elif issue_type == 'wrong_type' and 'amount' in item:
                # Converte número para string
                item['amount'] = str(item['amount'])
            
            elif issue_type == 'invalid_date':
                # Data inválida
                date_fields = [k for k in item.keys() if 'date' in k]
                if date_fields:
                    item[random.choice(date_fields)] = "invalid-date"
            
            elif issue_type == 'negative_amount' and 'amount' in item:
                # Valor negativo
                item['amount'] = -abs(item['amount'])
    
    return data

def main():
    """Função principal para gerar todos os dados"""
    print("Gerando dados realistas e complexos...")
    
    # Gerar IDs de clientes
    customer_ids = generate_customer_ids()
    print(f"✓ Gerados {len(customer_ids)} clientes únicos")
    
    # Gerar transações realistas
    transactions = generate_realistic_transactions(customer_ids)
    print(f"✓ Geradas {len(transactions)} transações realistas")
    
    # Gerar pagamentos complexos
    payments = generate_complex_payments(transactions)
    print(f"✓ Gerados {len(payments)} pagamentos com cenários complexos")
    
    # Adicionar problemas de qualidade de dados
    transactions = add_data_quality_issues(transactions, 0.03)
    payments = add_data_quality_issues(payments, 0.02)
    print("✓ Adicionados problemas de qualidade de dados para teste")
    
    # Salvar arquivos
    with open('transactions_realistic.json', 'w', encoding='utf-8') as f:
        json.dump(transactions, f, indent=2, ensure_ascii=False, default=str)
    
    with open('payments_realistic.json', 'w', encoding='utf-8') as f:
        json.dump(payments, f, indent=2, ensure_ascii=False, default=str)
    
    print("\n📊 Estatísticas dos dados gerados:")
    print(f"   • Clientes únicos: {len(set(t['customer_id'] for t in transactions))}")
    print(f"   • Transações: {len(transactions)}")
    print(f"   • Pagamentos: {len(payments)}")
    print(f"   • Transações sem pagamento: {len(transactions) - len(set(p['transaction_id'] for p in payments))}")
    
    # Estatísticas de inadimplência
    transaction_ids_with_payments = set(p['transaction_id'] for p in payments)
    unpaid_transactions = [t for t in transactions if t['transaction_id'] not in transaction_ids_with_payments]
    print(f"   • Taxa de inadimplência aparente: {len(unpaid_transactions)/len(transactions)*100:.1f}%")
    
    print("\n✅ Dados realistas gerados com sucesso!")
    print("   • transactions_realistic.json")
    print("   • payments_realistic.json")

if __name__ == "__main__":
    main()
